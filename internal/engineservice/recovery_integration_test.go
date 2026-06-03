package engineservice_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bookstate"
	"github.com/thorlaidanegg/clob-server/internal/engineservice"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/testutil"
	"github.com/thorlaidanegg/clob/types"
)

// TestRecoverReplay_RebuildsBookFromCheckpoint proves that the engine recovers a
// resting book from a durable checkpoint and that recovered orders are real,
// matchable book residents — with no wallet changes.
func TestRecoverReplay_RebuildsBookFromCheckpoint(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	ctx := context.Background()

	mc := testutil.DefaultConfig("BTC-USD")
	pp, qp := mc.PricePrecision, mc.QtyPrecision
	dec := func(s string, prec uint8) types.Decimal { return types.MustDecimal(s, prec) }

	// Build a checkpoint: alice bids 5 @ 100.00 (resting), bob asks 4 @ 101.00 (resting).
	st := bookstate.New()
	st.Apply(&events.OrderAccepted{Base: events.NewBase(1, 0, "BTC-USD"), OrderID: "o1", UserID: "alice",
		Side: types.Bid, OrderType: types.Limit, Price: dec("100.00", pp), OrigQty: dec("5", qp),
		DisplayQty: dec("5", qp), TIF: types.GTC, OrderSeqNum: 1})
	st.Apply(&events.OrderRested{Base: events.NewBase(2, 0, "BTC-USD"), OrderID: "o1", Side: types.Bid,
		Price: dec("100.00", pp), RemainQty: dec("5", qp), DisplayQty: dec("5", qp)})
	st.Apply(&events.OrderAccepted{Base: events.NewBase(3, 0, "BTC-USD"), OrderID: "o2", UserID: "bob",
		Side: types.Ask, OrderType: types.Limit, Price: dec("101.00", pp), OrigQty: dec("4", qp),
		DisplayQty: dec("4", qp), TIF: types.GTC, OrderSeqNum: 2})
	st.Apply(&events.OrderRested{Base: events.NewBase(4, 0, "BTC-USD"), OrderID: "o2", Side: types.Ask,
		Price: dec("101.00", pp), RemainQty: dec("4", qp), DisplayQty: dec("4", qp)})

	blob, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := pgstore.UpsertBookSnapshotTx(ctx, tx, pgstore.BookSnapshotRow{
		MarketID: "BTC-USD", LastEventSeq: st.LastEventSeq, State: blob,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Seed a wallet to confirm recovery leaves it untouched.
	if _, err := pool.Exec(ctx,
		`INSERT INTO wallets (user_id, available, reserved) VALUES ('alice', 1000, 500)`); err != nil {
		t.Fatal(err)
	}

	// Recover from the checkpoint (no Kafka tail in this test).
	recovered, initSeq := engineservice.RecoverReplay(ctx, pool, []clobconfig.MarketConfig{mc}, nil, zerolog.Nop())
	if len(recovered["BTC-USD"]) != 2 {
		t.Fatalf("recovered %d orders, want 2", len(recovered["BTC-USD"]))
	}
	if initSeq["BTC-USD"] != st.LastEventSeq+1 {
		t.Errorf("initialEventSeq = %d, want %d", initSeq["BTC-USD"], st.LastEventSeq+1)
	}

	// Build an engine seeded with the recovered orders.
	mc.InitialEventSeq = initSeq["BTC-USD"]
	e, err := engine.New(mc, engine.WithInitialOrders(recovered["BTC-USD"]),
		engine.WithCommandBuffer(256), engine.WithEventBuffer(1024))
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() }) //nolint

	bid, ask, hasBid, hasAsk := e.BBO()
	if !hasBid || !hasAsk || bid.String() != "100.00" || ask.String() != "101.00" {
		t.Fatalf("recovered BBO = %s/%s (hasBid=%v hasAsk=%v), want 100.00/101.00", bid, ask, hasBid, hasAsk)
	}

	// An aggressive ask at 100.00 must fill the recovered resting bid o1, and the
	// resulting events must carry seq numbers at/above the recovered initial seq.
	_ = e.Submit(engine.AdminResumeMarket{MarketID: "BTC-USD"})
	_ = e.Submit(engine.PlaceLimitOrder{MarketID: "BTC-USD", OrderID: types.NewOrderID(), UserID: "dave",
		Side: types.Ask, Price: dec("100.00", pp), Qty: dec("5", qp), TIF: types.GTC})

	var filled bool
	deadline := time.After(time.Second)
	for !filled {
		select {
		case ev := <-e.Events():
			if f, ok := ev.(events.TradeFill); ok && f.OrderID == "o1" {
				if f.SeqNum() < initSeq["BTC-USD"] {
					t.Errorf("post-recovery event seq %d below initial %d", f.SeqNum(), initSeq["BTC-USD"])
				}
				filled = true
			}
		case <-deadline:
			t.Fatal("recovered resting bid was never filled by the crossing ask")
		}
	}

	// Recovery must not have touched wallets.
	var avail, reserved int64
	if err := pool.QueryRow(ctx,
		`SELECT available, reserved FROM wallets WHERE user_id='alice'`).Scan(&avail, &reserved); err != nil {
		t.Fatal(err)
	}
	if avail != 1000 || reserved != 500 {
		t.Errorf("wallet changed during recovery: available=%d reserved=%d, want 1000/500", avail, reserved)
	}
}
