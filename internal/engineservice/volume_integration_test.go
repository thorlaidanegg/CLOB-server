package engineservice_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/engineservice"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/types"
)

func TestVolumeCache_RefreshAggregates30DayNotional(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	ctx := context.Background()

	// Two trades. Notional per trade = price*qty / 10^qtyPrec, at price precision 2.
	//  trade1: maker=alice taker=bob, 100.00 x 5.00 → 500.00 to each.
	//  trade2: maker=bob taker=alice, 100.00 x 2.00 → 200.00 to each.
	// alice = 700.00, bob = 700.00.
	mustInsertTrade(t, pool, pgstore.TradeRow{
		TradeID: "t1", MarketID: "BTC-USD", MakerOrderID: "a", TakerOrderID: "b",
		MakerUserID: "alice", TakerUserID: "bob", MakerSide: "bid",
		Price: 10000, Qty: 500, SeqNum: 1,
	})
	mustInsertTrade(t, pool, pgstore.TradeRow{
		TradeID: "t2", MarketID: "BTC-USD", MakerOrderID: "c", TakerOrderID: "d",
		MakerUserID: "bob", TakerUserID: "alice", MakerSide: "ask",
		Price: 10000, Qty: 200, SeqNum: 2,
	})

	cfg := clobconfig.MarketConfig{MarketID: "BTC-USD", PricePrecision: 2, QtyPrecision: 2}
	vc := engineservice.NewVolumeCache(pool, []clobconfig.MarketConfig{cfg}, zerolog.Nop())

	if err := vc.Refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	if got := vc.GetVolume(types.UserID("alice"), types.MarketID("BTC-USD")).String(); got != "700.00" {
		t.Errorf("alice volume = %s, want 700.00", got)
	}
	if got := vc.GetVolume(types.UserID("bob"), types.MarketID("BTC-USD")).String(); got != "700.00" {
		t.Errorf("bob volume = %s, want 700.00", got)
	}
	// Unknown user → zero at the market's price precision.
	if got := vc.GetVolume(types.UserID("nobody"), types.MarketID("BTC-USD")).String(); got != "0.00" {
		t.Errorf("unknown user volume = %s, want 0.00", got)
	}
}

func mustInsertTrade(t *testing.T, pool *pgxpool.Pool, row pgstore.TradeRow) {
	t.Helper()
	if err := pgstore.InsertTrade(context.Background(), pool, row); err != nil {
		t.Fatalf("insert trade %s: %v", row.TradeID, err)
	}
}
