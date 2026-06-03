package booksnapshot_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bookstate"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	"github.com/thorlaidanegg/clob-server/internal/workers/booksnapshot"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
)

func TestBookSnapshotHandler_PersistsCheckpoint(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	ctx := context.Background()

	h := booksnapshot.New(zerolog.Nop())
	dec := func(s string, p uint8) types.Decimal { return types.MustDecimal(s, p) }

	apply := func(ev events.Event, seq uint64, off int64) {
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := h.HandleEvent(ctx, tx, workers.EventEnvelope{
			Event: ev, MarketID: "BTC-USD", SeqNum: seq, Offset: off,
		}); err != nil {
			tx.Rollback(ctx) //nolint
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// A resting bid, then a partial maker fill leaving remain 3.
	apply(&events.OrderAccepted{Base: events.NewBase(1, 0, "BTC-USD"), OrderID: "o1", UserID: "alice",
		Side: types.Bid, OrderType: types.Limit, Price: dec("100.00", 2), OrigQty: dec("5", 0),
		DisplayQty: dec("5", 0), TIF: types.GTC, OrderSeqNum: 1}, 1, 10)
	apply(&events.OrderRested{Base: events.NewBase(2, 0, "BTC-USD"), OrderID: "o1", Side: types.Bid,
		Price: dec("100.00", 2), RemainQty: dec("5", 0), DisplayQty: dec("5", 0)}, 2, 11)
	apply(&events.TradeFill{Base: events.NewBase(3, 0, "BTC-USD"), OrderID: "o1", UserID: "alice",
		Role: events.RoleMaker, Side: types.Bid, Price: dec("100.00", 2),
		FilledQty: dec("2", 0), RemainQty: dec("3", 0)}, 3, 12)

	row, ok, err := pgstore.GetBookSnapshot(ctx, pool, "BTC-USD")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("no checkpoint persisted")
	}
	if row.LastEventSeq != 3 || row.KafkaOffset != 12 {
		t.Errorf("checkpoint seq/offset = %d/%d, want 3/12", row.LastEventSeq, row.KafkaOffset)
	}

	st := bookstate.New()
	if err := json.Unmarshal(row.State, st); err != nil {
		t.Fatal(err)
	}
	got := st.ToRecovered()
	if len(got) != 1 || got[0].OrderID != "o1" || got[0].RemainQty.String() != "3" {
		t.Fatalf("persisted state = %+v, want 1 order o1 remain 3", got)
	}

	// LoadSnapshots must read the checkpoint back without error.
	if err := booksnapshot.New(zerolog.Nop()).LoadSnapshots(ctx, pool); err != nil {
		t.Fatalf("LoadSnapshots: %v", err)
	}
}
