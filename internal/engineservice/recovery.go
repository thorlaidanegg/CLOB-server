package engineservice

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/types"
)

// RecoverOpenOrders cancels all orders that were open before the engine restarted.
//
// The engine starts with an empty book. Any order marked rested/new/partial in
// Postgres was lost when the process exited. For each such order this function:
//   - Releases the reserved wallet credits back to available
//   - Marks the order as canceled
//
// Both writes happen in a single Postgres transaction so a crash mid-recovery
// is safe to retry: the order will still be open and recovery will re-process it.
//
// Called synchronously from Run() before the gRPC server opens and before any
// live commands are accepted, so there is no race with the matching engine.
func RecoverOpenOrders(ctx context.Context, pool *pgxpool.Pool, marketCfgs []clobconfig.MarketConfig, log zerolog.Logger) {
	for _, mc := range marketCfgs {
		n, err := recoverMarket(ctx, pool, mc)
		if err != nil {
			log.Error().Err(err).Str("market", string(mc.MarketID)).Msg("recovery: failed for market")
			continue
		}
		if n > 0 {
			log.Warn().Str("market", string(mc.MarketID)).Int("orders", n).
				Msg("recovery: canceled open orders from previous run — users must re-enter")
		}
	}
}

// openOrderRow carries the minimal data needed to compute the credit release.
type openOrderRow struct {
	OrderID         string
	UserID          string
	Price           int64
	RemainQty       int64
	ReservedPerUnit int64
}

func recoverMarket(ctx context.Context, pool *pgxpool.Pool, mc clobconfig.MarketConfig) (int, error) {
	rows, err := pool.Query(ctx,
		`SELECT order_id, user_id, COALESCE(price,0), remain_qty, reserved_per_unit
		 FROM orders
		 WHERE market_id = $1
		   AND status IN ('new', 'rested', 'partial')`,
		string(mc.MarketID),
	)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var open []openOrderRow
	for rows.Next() {
		var o openOrderRow
		if err := rows.Scan(&o.OrderID, &o.UserID, &o.Price, &o.RemainQty, &o.ReservedPerUnit); err != nil {
			return 0, err
		}
		open = append(open, o)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	pp := mc.PricePrecision
	qp := mc.QtyPrecision

	for _, o := range open {
		if err := cancelOrderTx(ctx, pool, o, pp, qp); err != nil {
			// Log and continue — one bad order should not block recovery of others.
			return 0, err
		}
	}
	return len(open), nil
}

func cancelOrderTx(ctx context.Context, pool *pgxpool.Pool, o openOrderRow, pp, qp uint8) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Release only what the hook reserved. Only buys reserve credits
	// (reserved_per_unit > 0); sells reserved nothing, so they release nothing —
	// never fall back to price × qty (that would wrongly refund a seller).
	// See doc/WALLET_MODEL.md §4.
	var releaseRaw int64
	if o.ReservedPerUnit > 0 {
		qty := types.NewDecimal(o.RemainQty, qp)
		reservedPerUnit := types.NewDecimal(o.ReservedPerUnit, pp)
		releaseRaw = reservedPerUnit.Mul(qty).Value()
	}

	if releaseRaw > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE wallets
			 SET reserved  = reserved  - $2,
			     available = available + $2,
			     version   = version   + 1,
			     updated_at = now()
			 WHERE user_id = $1
			   AND reserved >= $2`,
			o.UserID, releaseRaw,
		); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(ctx,
		`UPDATE orders SET status = 'canceled', updated_at = now()
		 WHERE order_id = $1`,
		o.OrderID,
	); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
