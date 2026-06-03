package leaderboard

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
)

// Handler maintains a real-time leaderboard by tracking per-user PnL deltas.
type Handler struct {
	pool        *pgxpool.Pool
	rdb         *redis.Client
	marketCache map[string]clobconfig.MarketConfig
	// avgCache and qtyCache are seeded from Postgres on startup to avoid reading
	// from the positions table during fill processing (race with portfolio worker).
	avgCache map[string]int64 // "userID:marketID" → raw avg_entry_price
	qtyCache map[string]int64 // "userID:marketID" → raw quantity
	logger   zerolog.Logger
}

// New creates a leaderboard Handler. Seed avgCache and qtyCache before calling Run.
func New(pool *pgxpool.Pool, rdb *redis.Client, marketCache map[string]clobconfig.MarketConfig, logger zerolog.Logger) (*Handler, error) {
	h := &Handler{
		pool:        pool,
		rdb:         rdb,
		marketCache: marketCache,
		avgCache:    make(map[string]int64),
		qtyCache:    make(map[string]int64),
		logger:      logger,
	}

	// Seed from Postgres on startup.
	rows, err := pgstore.ListAllPositions(context.Background(), pool)
	if err != nil {
		return nil, fmt.Errorf("leaderboard: seed positions: %w", err)
	}
	for _, r := range rows {
		key := r.UserID + ":" + r.MarketID
		h.avgCache[key] = r.AvgEntryPrice
		h.qtyCache[key] = r.Quantity
	}
	return h, nil
}

// HandleEvent processes trade fills to update the leaderboard.
func (h *Handler) HandleEvent(ctx context.Context, _ pgx.Tx, env workers.EventEnvelope) error {
	if env.EventType != events.TypeTradeFill {
		return nil
	}
	fill, ok := env.Event.(*events.TradeFill)
	if !ok {
		return nil
	}

	market, ok := h.marketCache[string(fill.MarketID())]
	if !ok {
		return nil
	}
	pp := market.PricePrecision
	qp := market.QtyPrecision

	userID := string(fill.UserID)
	marketID := string(fill.MarketID())
	key := userID + ":" + marketID

	if fill.Side == types.Bid {
		// Buy: update the buyer's avg entry price and qty in the local cache.
		existingAvg := types.NewDecimal(h.avgCache[key], pp)
		existingQty := types.NewDecimal(h.qtyCache[key], qp)
		newQty := existingQty.Add(fill.FilledQty)
		if newQty.Value() != 0 {
			numerator := existingAvg.MulQty(existingQty).Add(fill.Price.MulQty(fill.FilledQty))
			newAvg := numerator.Div(newQty, pp)
			h.avgCache[key] = newAvg.Value()
		}
		h.qtyCache[key] = newQty.Value()
		return nil
	}

	// Sell: realise PnL and push to Redis leaderboard.
	avgDec := types.NewDecimal(h.avgCache[key], pp)
	fillPnL := fill.Price.Sub(avgDec).MulQty(fill.FilledQty)

	if err := redisstore.ZIncrBy(ctx, h.rdb, "leaderboard", float64(fillPnL.Value()), userID); err != nil {
		h.logger.Warn().Err(err).Str("userID", userID).Msg("leaderboard: ZIncrBy global failed")
	}
	if err := redisstore.ZIncrBy(ctx, h.rdb, "leaderboard:"+marketID, float64(fillPnL.Value()), userID); err != nil {
		h.logger.Warn().Err(err).Str("userID", userID).Msg("leaderboard: ZIncrBy market failed")
	}

	// Update seller's qty cache.
	sellerQty := types.NewDecimal(h.qtyCache[key], qp)
	newQty := sellerQty.Sub(fill.FilledQty)
	h.qtyCache[key] = newQty.Value()
	return nil
}
