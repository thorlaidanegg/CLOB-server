package engineservice

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bookstate"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	"github.com/thorlaidanegg/clob/engine"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/types"
)

// --- Replay recovery (default) -------------------------------------------
//
// The engine's own market-events log is the source of truth. On startup we load
// the latest per-market checkpoint (a compaction of that log, maintained by the
// booksnapshot worker and durable in Postgres) and, when a Kafka tail consumer is
// supplied, fold any events past the checkpoint to close the final gap. The
// resulting resting orders seed the engine via WithInitialOrders; reserved
// credits never moved during a crash, so no wallet changes are needed.

// idleDrainTimeout is how long the tail reader waits for a new event before
// concluding it has caught up to the head of the log.
const idleDrainTimeout = 2 * time.Second

// RecoverReplay rebuilds each market's resting book from its checkpoint, folding
// the Kafka event tail when consumer is non-nil. It returns the recovered orders
// per market and the initial event sequence (last folded seq + 1) so the engine's
// event counter continues above already-published events and never collides with
// worker idempotency. consumer is closed by the caller.
func RecoverReplay(
	ctx context.Context,
	pool *pgxpool.Pool,
	marketCfgs []clobconfig.MarketConfig,
	consumer bus.Consumer,
	log zerolog.Logger,
) (map[string][]engine.RecoveredOrder, map[string]uint64) {
	states := make(map[string]*bookstate.BookState, len(marketCfgs))
	known := make(map[string]bool, len(marketCfgs))
	for _, mc := range marketCfgs {
		id := string(mc.MarketID)
		known[id] = true
		st := bookstate.New()
		if row, ok, err := pgstore.GetBookSnapshot(ctx, pool, id); err != nil {
			log.Error().Err(err).Str("market", id).Msg("recovery: load checkpoint failed, starting fresh")
		} else if ok {
			if uerr := json.Unmarshal(row.State, st); uerr != nil {
				log.Error().Err(uerr).Str("market", id).Msg("recovery: corrupt checkpoint, starting fresh")
				st = bookstate.New()
			}
		}
		states[id] = st
	}

	if consumer != nil {
		drainFold(ctx, consumer, states, known, log)
	}

	recovered := make(map[string][]engine.RecoveredOrder, len(states))
	initSeq := make(map[string]uint64, len(states))
	for id, st := range states {
		recovered[id] = st.ToRecovered()
		initSeq[id] = st.LastEventSeq + 1
		if n := len(recovered[id]); n > 0 {
			log.Info().Str("market", id).Int("orders", n).Uint64("throughSeq", st.LastEventSeq).
				Msg("recovery: rebuilt resting book from event log")
		}
	}
	return recovered, initSeq
}

// drainFold reads market-events from the beginning and folds each event into the
// matching market's state, stopping once the log is drained (no new event within
// idleDrainTimeout). The fold skips events at or below each checkpoint's seq, so
// re-reading the retained log is cheap and idempotent.
func drainFold(ctx context.Context, consumer bus.Consumer, states map[string]*bookstate.BookState, known map[string]bool, log zerolog.Logger) {
	if err := consumer.Subscribe("market-events", ""); err != nil {
		log.Error().Err(err).Msg("recovery: subscribe failed; checkpoint-only recovery")
		return
	}

	var folded int
	for {
		pctx, cancel := context.WithTimeout(ctx, idleDrainTimeout)
		msg, err := consumer.Poll(pctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return // parent canceled
			}
			break // idle timeout — log drained
		}
		if len(msg.Value) == 0 || !known[msg.Key] {
			continue
		}
		ev, derr := workers.DeserializeEvent(msg.Headers["event-type"], msg.Value)
		if derr != nil || ev == nil {
			continue
		}
		states[msg.Key].Apply(ev)
		folded++
	}
	log.Info().Int("events", folded).Msg("recovery: folded event tail")
}

// --- Cancel recovery (ENGINE_RECOVERY=cancel fallback) -------------------

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
		releaseRaw = reservedPerUnit.MulQty(qty).Value()
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
