package portfolio

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

// Handler processes portfolio events.
type Handler struct {
	pool        *pgxpool.Pool
	rdb         *redis.Client
	marketCache map[string]clobconfig.MarketConfig
	logger      zerolog.Logger
}

// New creates a portfolio Handler with a pre-loaded market cache.
func New(pool *pgxpool.Pool, rdb *redis.Client, marketCache map[string]clobconfig.MarketConfig, logger zerolog.Logger) *Handler {
	return &Handler{pool: pool, rdb: rdb, marketCache: marketCache, logger: logger}
}

// HandleEvent processes trade fills to update positions.
func (h *Handler) HandleEvent(ctx context.Context, tx pgx.Tx, env workers.EventEnvelope) error {
	if env.EventType != events.TypeTradeFill {
		return nil
	}
	fill, ok := env.Event.(*events.TradeFill)
	if !ok {
		return nil
	}

	market, ok := h.marketCache[string(fill.MarketID())]
	if !ok {
		return fmt.Errorf("portfolio: unknown market %s", fill.MarketID())
	}

	userID := string(fill.UserID)
	marketID := string(fill.MarketID())
	pp := market.PricePrecision
	qp := market.QtyPrecision

	if fill.Side == types.Bid {
		return h.handleBuy(ctx, tx, userID, marketID, fill, pp, qp)
	}
	return h.handleSell(ctx, tx, userID, marketID, fill, pp, qp)
}

func (h *Handler) handleBuy(ctx context.Context, tx pgx.Tx, userID, marketID string, fill *events.TradeFill, pp, qp uint8) error {
	row, err := pgstore.GetPosition(ctx, h.pool, userID, marketID)
	if err != nil {
		return err
	}

	existingAvg := types.NewDecimal(row.AvgEntryPrice, pp)
	existingQty := types.NewDecimal(row.Quantity, qp)
	newQty := existingQty.Add(fill.FilledQty)
	numerator := existingAvg.Mul(existingQty).Add(fill.Price.Mul(fill.FilledQty))
	var newAvg types.Decimal
	if newQty.Value() != 0 {
		newAvg = numerator.Div(newQty, pp)
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO positions (user_id, market_id, quantity, avg_entry_price)
		 VALUES ($1,$2,$3,$4)
		 ON CONFLICT (user_id, market_id) DO UPDATE SET
		   quantity        = $3,
		   avg_entry_price = $4,
		   updated_at      = now()`,
		userID, marketID, newQty.Value(), newAvg.Value(),
	)
	if err != nil {
		return fmt.Errorf("portfolio: upsert position on buy: %w", err)
	}

	redisstore.SetLastPrice(ctx, h.rdb, marketID, fill.Price.String())
	return nil
}

func (h *Handler) handleSell(ctx context.Context, tx pgx.Tx, userID, marketID string, fill *events.TradeFill, pp, qp uint8) error {
	var qRaw, avgRaw, pnlRaw int64
	err := tx.QueryRow(ctx,
		`SELECT quantity, avg_entry_price, realised_pnl
		 FROM positions WHERE user_id=$1 AND market_id=$2 FOR UPDATE`,
		userID, marketID,
	).Scan(&qRaw, &avgRaw, &pnlRaw)
	if err != nil {
		return fmt.Errorf("portfolio: lock position on sell: %w", err)
	}

	existingAvg := types.NewDecimal(avgRaw, pp)
	existingPnL := types.NewDecimal(pnlRaw, pp)
	existingQty := types.NewDecimal(qRaw, qp)

	fillPnL := fill.Price.Sub(existingAvg).Mul(fill.FilledQty)
	newPnL := existingPnL.Add(fillPnL)
	newQty := existingQty.Sub(fill.FilledQty)

	_, err = tx.Exec(ctx,
		`UPDATE positions SET quantity=$3, realised_pnl=$4, updated_at=now()
		 WHERE user_id=$1 AND market_id=$2`,
		userID, marketID, newQty.Value(), newPnL.Value(),
	)
	if err != nil {
		return fmt.Errorf("portfolio: update position on sell: %w", err)
	}

	redisstore.SetLastPrice(ctx, h.rdb, marketID, fill.Price.String())
	return nil
}
