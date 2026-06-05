// Command reconcile repairs wallet/order state corrupted by dead-lettered
// settlement events (see internal/store/postgres dead_letter_events).
//
// It is safe and idempotent. By default it is a DRY RUN (computes and prints the
// changes, then rolls back). Pass -apply to commit.
//
//	POSTGRES_DSN=postgres://clob:clob@localhost:5432/clob?sslmode=disable \
//	  go run ./cmd/reconcile          # dry run
//	  go run ./cmd/reconcile -apply   # commit
//
// Three repairs, in order:
//  1. Re-insert trades dropped by dead-lettered trade_executed events (the wallet
//     was already settled by TradeFill events; only the trade row was missing).
//  2. Close "ghost" orders — open in the DB but already terminated in the engine,
//     left open because their cancel/reject settlement dead-lettered.
//  3. Recompute every wallet's reserved from its remaining open buy orders and fix
//     the reserved/available split (total equity is preserved).
package main

import (
	"context"
	"flag"
	"os"

	"github.com/thorlaidanegg/clob-server/internal/shared/logger"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
)

func main() {
	apply := flag.Bool("apply", false, "commit changes (default: dry run, rolled back)")
	flag.Parse()

	ctx := context.Background()
	log := logger.New("info", "development")

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		log.Fatal().Msg("POSTGRES_DSN is required")
	}
	pool, err := pgstore.Connect(ctx, dsn)
	if err != nil {
		log.Fatal().Err(err).Msg("connect postgres")
	}
	defer pool.Close()

	// Market precisions for the reservation math.
	markets, err := pgstore.ListMarkets(ctx, pool)
	if err != nil {
		log.Fatal().Err(err).Msg("list markets")
	}
	type prec struct{ pp, qp uint8 }
	precByMarket := make(map[string]prec, len(markets))
	for _, m := range markets {
		precByMarket[m.MarketID] = prec{m.PricePrecision, m.QtyPrecision}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("begin tx")
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// ── 1. Re-insert trades dropped by dead-lettered trade_executed events ──────
	tradeDL, err := tx.Query(ctx,
		`SELECT id, payload FROM dead_letter_events WHERE event_type=$1`, events.TypeTradeExecuted)
	if err != nil {
		log.Fatal().Err(err).Msg("query trade dead-letters")
	}
	type dlTrade struct {
		id      int64
		payload []byte
	}
	var trades []dlTrade
	for tradeDL.Next() {
		var t dlTrade
		if err := tradeDL.Scan(&t.id, &t.payload); err != nil {
			log.Fatal().Err(err).Msg("scan trade dead-letter")
		}
		trades = append(trades, t)
	}
	tradeDL.Close()

	tradesReinserted := 0
	for _, t := range trades {
		ev, err := workers.DeserializeEvent(events.TypeTradeExecuted, t.payload)
		if err != nil || ev == nil {
			log.Error().Int64("dlID", t.id).Msg("reconcile: cannot decode trade_executed payload — skipping")
			continue
		}
		te := ev.(*events.TradeExecuted)
		if err := pgstore.InsertTradeTx(ctx, tx, pgstore.TradeRow{
			TradeID:      string(te.TradeID),
			MarketID:     string(te.MarketID()),
			MakerOrderID: string(te.MakerOrderID),
			TakerOrderID: string(te.TakerOrderID),
			MakerUserID:  string(te.MakerUserID),
			TakerUserID:  string(te.TakerUserID),
			MakerSide:    te.MakerSide.String(),
			Price:        te.Price.Value(),
			Qty:          te.Qty.Value(),
			MakerFee:     te.MakerFee.Value(),
			TakerFee:     te.TakerFee.Value(),
			FeeCurrency:  te.FeeCurrency,
			SeqNum:       int64(te.SeqNum()),
			ExecutedAtNs: te.Timestamp(),
		}); err != nil {
			log.Fatal().Err(err).Str("tradeID", string(te.TradeID)).Msg("reconcile: re-insert trade")
		}
		log.Info().Str("tradeID", string(te.TradeID)).Str("market", string(te.MarketID())).Msg("reconcile: re-inserted dropped trade")
		tradesReinserted++
	}

	// ── 2. Close ghost orders (open in DB, terminal event dead-lettered) ────────
	ghostTag, err := tx.Exec(ctx,
		`UPDATE orders SET status='canceled', updated_at=now()
		 WHERE status IN ('new','rested','partial')
		   AND order_id IN (
		     SELECT convert_from(payload,'UTF8')::json->>'orderID'
		     FROM dead_letter_events
		     WHERE event_type IN ('order_canceled','order_rejected','order_expired')
		   )`)
	if err != nil {
		log.Fatal().Err(err).Msg("reconcile: close ghost orders")
	}
	log.Info().Int64("count", ghostTag.RowsAffected()).Msg("reconcile: closed ghost orders")

	// ── 3. Recompute each wallet's reserved from its open buy orders ────────────
	openRows, err := tx.Query(ctx,
		`SELECT user_id, market_id, reserved_per_unit, remain_qty
		 FROM orders
		 WHERE status IN ('new','rested','partial') AND side='bid' AND reserved_per_unit > 0`)
	if err != nil {
		log.Fatal().Err(err).Msg("reconcile: query open buy orders")
	}
	wantReserved := make(map[string]int64) // user_id → reserved raw (wallet precision)
	for openRows.Next() {
		var userID, marketID string
		var reservedPerUnit, remainQty int64
		if err := openRows.Scan(&userID, &marketID, &reservedPerUnit, &remainQty); err != nil {
			log.Fatal().Err(err).Msg("reconcile: scan open order")
		}
		p := precByMarket[marketID]
		release := types.NewDecimal(reservedPerUnit, p.pp).MulQty(types.NewDecimal(remainQty, p.qp))
		wantReserved[userID] += release.Value()
	}
	openRows.Close()

	walletRows, err := tx.Query(ctx, `SELECT user_id, available, reserved FROM wallets`)
	if err != nil {
		log.Fatal().Err(err).Msg("reconcile: query wallets")
	}
	type fix struct {
		userID                       string
		oldAvail, oldRes, newAvail, newRes int64
	}
	var fixes []fix
	for walletRows.Next() {
		var userID string
		var available, reserved int64
		if err := walletRows.Scan(&userID, &available, &reserved); err != nil {
			log.Fatal().Err(err).Msg("reconcile: scan wallet")
		}
		want := wantReserved[userID]
		if reserved != want {
			delta := reserved - want // moved from reserved back to available
			fixes = append(fixes, fix{userID, available, reserved, available + delta, want})
		}
	}
	walletRows.Close()

	for _, f := range fixes {
		log.Warn().
			Str("userID", f.userID).
			Int64("reserved_old", f.oldRes).Int64("reserved_new", f.newRes).
			Int64("available_old", f.oldAvail).Int64("available_new", f.newAvail).
			Msg("reconcile: fixing wallet reserved/available split")
		if _, err := tx.Exec(ctx,
			`UPDATE wallets SET reserved=$2, available=$3, version=version+1, updated_at=now() WHERE user_id=$1`,
			f.userID, f.newRes, f.newAvail,
		); err != nil {
			log.Fatal().Err(err).Str("userID", f.userID).Msg("reconcile: update wallet")
		}
	}

	// ── 4. Clear the dead-letters we have now reconciled ────────────────────────
	delTag, err := tx.Exec(ctx,
		`DELETE FROM dead_letter_events
		 WHERE event_type IN ('trade_executed','order_canceled','order_rejected','order_expired')`)
	if err != nil {
		log.Fatal().Err(err).Msg("reconcile: clear dead-letters")
	}

	log.Info().
		Int("trades_reinserted", tradesReinserted).
		Int64("ghost_orders_closed", ghostTag.RowsAffected()).
		Int("wallets_fixed", len(fixes)).
		Int64("dead_letters_cleared", delTag.RowsAffected()).
		Bool("apply", *apply).
		Msg("reconcile: summary")

	if *apply {
		if err := tx.Commit(ctx); err != nil {
			log.Fatal().Err(err).Msg("commit")
		}
		log.Info().Msg("reconcile: committed")
	} else {
		log.Info().Msg("reconcile: DRY RUN — rolled back; pass -apply to commit")
	}
}
