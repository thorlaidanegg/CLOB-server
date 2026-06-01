package engineservice

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/types"
)

// VolumeCache implements fees.VolumeProvider. The engine calls GetVolume inside
// the hot matching loop, so it must be served from memory — never a live query.
// A background goroutine periodically aggregates 30-day traded notional per
// (user, market) from the trades table and swaps it into the cache.
type VolumeCache struct {
	pool       *pgxpool.Pool
	logger     zerolog.Logger
	precisions map[string]uint8 // marketID -> price precision
	qtyPrec    map[string]uint8 // marketID -> qty precision

	mu  sync.RWMutex
	vol map[string]int64 // "marketID|userID" -> notional at the market's price precision
}

// NewVolumeCache builds a cache covering the given markets.
func NewVolumeCache(pool *pgxpool.Pool, marketCfgs []clobconfig.MarketConfig, logger zerolog.Logger) *VolumeCache {
	vc := &VolumeCache{
		pool:       pool,
		logger:     logger,
		precisions: make(map[string]uint8, len(marketCfgs)),
		qtyPrec:    make(map[string]uint8, len(marketCfgs)),
		vol:        make(map[string]int64),
	}
	for _, mc := range marketCfgs {
		vc.precisions[string(mc.MarketID)] = mc.PricePrecision
		vc.qtyPrec[string(mc.MarketID)] = mc.QtyPrecision
	}
	return vc
}

// GetVolume returns a user's 30-day traded notional for a market, at the market's
// price precision. Returns zero if the user has no recorded volume. Lock-free for
// readers except for a short RLock; safe to call from the matching loop.
func (vc *VolumeCache) GetVolume(userID types.UserID, marketID types.MarketID) types.Decimal {
	pp := vc.precisions[string(marketID)]
	vc.mu.RLock()
	raw := vc.vol[string(marketID)+"|"+string(userID)]
	vc.mu.RUnlock()
	return types.NewDecimal(raw, pp)
}

// Refresh recomputes 30-day volume for every market and atomically swaps it in.
func (vc *VolumeCache) Refresh(ctx context.Context) error {
	next := make(map[string]int64)
	for marketID, qp := range vc.qtyPrec {
		if err := vc.refreshMarket(ctx, marketID, qp, next); err != nil {
			return err
		}
	}
	vc.mu.Lock()
	vc.vol = next
	vc.mu.Unlock()
	return nil
}

func (vc *VolumeCache) refreshMarket(ctx context.Context, marketID string, qtyPrec uint8, out map[string]int64) error {
	// A user's volume counts trades where they were maker OR taker. Notional is
	// price*qty normalized to price precision: divide the summed price*qty product
	// by 10^qtyPrec. SUM is done in NUMERIC to avoid int64 overflow, then cast to
	// BIGINT (at price precision).
	qtyScale := int64(1)
	for i := uint8(0); i < qtyPrec; i++ {
		qtyScale *= 10
	}

	rows, err := vc.pool.Query(ctx,
		`SELECT u, (SUM(price::numeric * qty::numeric) / $2)::bigint AS notional
		 FROM (
		   SELECT maker_user_id AS u, price, qty FROM trades
		     WHERE market_id = $1 AND created_at > now() - interval '30 days'
		   UNION ALL
		   SELECT taker_user_id AS u, price, qty FROM trades
		     WHERE market_id = $1 AND created_at > now() - interval '30 days'
		 ) t
		 GROUP BY u`,
		marketID, qtyScale,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var userID string
		var notional int64
		if err := rows.Scan(&userID, &notional); err != nil {
			return err
		}
		out[marketID+"|"+userID] = notional
	}
	return rows.Err()
}

// Run refreshes the cache once immediately, then on every tick until ctx is done.
func (vc *VolumeCache) Run(ctx context.Context, interval time.Duration) {
	if err := vc.Refresh(ctx); err != nil && ctx.Err() == nil {
		vc.logger.Warn().Err(err).Msg("volume cache: initial refresh failed")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := vc.Refresh(ctx); err != nil && ctx.Err() == nil {
				vc.logger.Warn().Err(err).Msg("volume cache: refresh failed")
			}
		}
	}
}
