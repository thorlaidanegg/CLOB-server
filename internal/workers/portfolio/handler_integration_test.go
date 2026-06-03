package portfolio_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	"github.com/thorlaidanegg/clob-server/internal/workers/portfolio"
)

func TestPortfolioHandler_BuyThenSell(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	rdb := testsupport.RequireMiniRedis(t)
	ctx := context.Background()

	// Self-consistent market: price and qty share precision 2 (see note in the
	// package test about the cross-precision Mul panic in the handlers).
	cache := map[string]clobconfig.MarketConfig{"BTC-USD": {MarketID: "BTC-USD", PricePrecision: 2, QtyPrecision: 2}}
	h := portfolio.New(pool, rdb, cache, zerolog.Nop())

	fill := func(side types.Side, price, qty string, seq uint64) *events.TradeFill {
		return &events.TradeFill{
			Base: events.NewBase(seq, 0, "BTC-USD"), OrderID: "o", UserID: "alice", Role: events.RoleTaker,
			Side: side, Price: types.MustDecimal(price, 2), FilledQty: types.MustDecimal(qty, 2),
			RemainQty: types.MustDecimal("0.00", 2),
		}
	}
	apply := func(f *events.TradeFill) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.HandleEvent(ctx, tx, workers.EventEnvelope{
			Event: f, EventType: events.TypeTradeFill, MarketID: "BTC-USD",
		}); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	readPos := func() (qty, avg, pnl int64) {
		if err := pool.QueryRow(ctx,
			`SELECT quantity, avg_entry_price, realised_pnl FROM positions WHERE user_id='alice' AND market_id='BTC-USD'`).
			Scan(&qty, &avg, &pnl); err != nil && err != pgx.ErrNoRows {
			t.Fatal(err)
		}
		return
	}

	// Buy 5 @ 100.00 → qty 5.00 (raw 500), avg 100.00 (raw 10000).
	apply(fill(types.Bid, "100.00", "5.00", 1))
	if q, a, _ := readPos(); q != 500 || a != 10000 {
		t.Fatalf("after first buy: qty=%d avg=%d, want 500/10000", q, a)
	}

	// Buy 5 @ 102.00 → qty 10.00, weighted avg 101.00.
	apply(fill(types.Bid, "102.00", "5.00", 2))
	if q, a, _ := readPos(); q != 1000 || a != 10100 {
		t.Fatalf("after second buy: qty=%d avg=%d, want 1000/10100", q, a)
	}

	// Sell 4 @ 110.00 → qty 6.00, realised PnL (110-101)*4 = 36.00 (raw 3600).
	apply(fill(types.Ask, "110.00", "4.00", 3))
	if q, _, p := readPos(); q != 600 || p != 3600 {
		t.Fatalf("after sell: qty=%d pnl=%d, want 600/3600", q, p)
	}

	// Last price is cached in Redis.
	if lp, ok, _ := redisstore.GetLastPrice(ctx, rdb, "BTC-USD"); !ok || lp != "110.00" {
		t.Errorf("lastprice = %q (ok=%v), want 110.00", lp, ok)
	}
}
