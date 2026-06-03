package leaderboard_test

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/testutil"
	"github.com/thorlaidanegg/clob/types"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	"github.com/thorlaidanegg/clob-server/internal/workers/leaderboard"
)

func TestLeaderboardHandler_RealisesPnLOnSell(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	rdb := testsupport.RequireMiniRedis(t)
	ctx := context.Background()

	// Realistic market with price precision 2 and qty precision 0 (pp != qp),
	// exercising the cross-precision PnL path (MulQty).
	cache := map[string]clobconfig.MarketConfig{"BTC-USD": testutil.DefaultConfig("BTC-USD")}
	h, err := leaderboard.New(pool, rdb, cache, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}

	fill := func(side types.Side, price, qty string, seq uint64) *events.TradeFill {
		return &events.TradeFill{
			Base: events.NewBase(seq, 0, "BTC-USD"), OrderID: "o", UserID: "alice",
			Side: side, Price: types.MustDecimal(price, 2), FilledQty: types.MustDecimal(qty, 0),
			RemainQty: types.MustDecimal("0", 0),
		}
	}
	apply := func(f *events.TradeFill) {
		if err := h.HandleEvent(ctx, nil, workers.EventEnvelope{
			Event: f, EventType: events.TypeTradeFill, MarketID: "BTC-USD",
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Buy 5 @ 100.00 establishes the cached entry price (no leaderboard change).
	apply(fill(types.Bid, "100.00", "5", 1))
	if _, err := rdb.ZScore(ctx, "leaderboard", "alice").Result(); err == nil {
		t.Fatal("buy should not put a score on the leaderboard yet")
	}

	// Sell 5 @ 110.00 realises PnL (110-100)*5 = 50.00 → raw 5000.
	apply(fill(types.Ask, "110.00", "5", 2))
	score, err := rdb.ZScore(ctx, "leaderboard", "alice").Result()
	if err != nil {
		t.Fatalf("ZScore: %v", err)
	}
	if score != 5000 {
		t.Errorf("leaderboard score = %v, want 5000", score)
	}
	// Per-market leaderboard mirrors the global one.
	if ms, err := rdb.ZScore(ctx, "leaderboard:BTC-USD", "alice").Result(); err != nil || ms != 5000 {
		t.Errorf("per-market score = %v (err=%v), want 5000", ms, err)
	}
}
