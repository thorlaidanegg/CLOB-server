package bookstate

import (
	"encoding/json"
	"testing"

	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
)

const (
	pp = 2 // price precision
	qp = 0 // qty precision
)

func base(seq uint64) events.Base { return events.NewBase(seq, 0, "BTC-USD") }

func dec(s string, prec uint8) types.Decimal { return types.MustDecimal(s, prec) }

// foldAll applies a sequence of events in order.
func foldAll(evs ...events.Event) *BookState {
	s := New()
	for _, e := range evs {
		s.Apply(e)
	}
	return s
}

func TestFold_RestingLimitSurvives(t *testing.T) {
	s := foldAll(
		&events.OrderAccepted{Base: base(1), OrderID: "o1", UserID: "alice", Side: types.Bid,
			OrderType: types.Limit, Price: dec("100.00", pp), OrigQty: dec("5", qp),
			DisplayQty: dec("5", qp), TIF: types.GTC, OrderSeqNum: 1},
		&events.OrderRested{Base: base(2), OrderID: "o1", UserID: "alice", Side: types.Bid,
			Price: dec("100.00", pp), RemainQty: dec("5", qp), DisplayQty: dec("5", qp)},
	)

	got := s.ToRecovered()
	if len(got) != 1 {
		t.Fatalf("want 1 resting order, got %d", len(got))
	}
	if got[0].OrderID != "o1" || got[0].RemainQty.String() != "5" || got[0].Price.String() != "100.00" {
		t.Errorf("unexpected resting order: %+v", got[0])
	}
	if s.LastEventSeq != 2 {
		t.Errorf("LastEventSeq = %d, want 2", s.LastEventSeq)
	}
}

func TestFold_PartialFillThenCancel(t *testing.T) {
	// Resting bid for 5, a maker fill takes 2 (remain 3), then user cancels → gone.
	s := foldAll(
		&events.OrderAccepted{Base: base(1), OrderID: "o1", UserID: "alice", Side: types.Bid,
			OrderType: types.Limit, Price: dec("100.00", pp), OrigQty: dec("5", qp),
			DisplayQty: dec("5", qp), TIF: types.GTC, OrderSeqNum: 1},
		&events.OrderRested{Base: base(2), OrderID: "o1", Side: types.Bid,
			Price: dec("100.00", pp), RemainQty: dec("5", qp), DisplayQty: dec("5", qp)},
		&events.TradeFill{Base: base(3), OrderID: "o1", UserID: "alice", Role: events.RoleMaker,
			Side: types.Bid, Price: dec("100.00", pp), FilledQty: dec("2", qp), RemainQty: dec("3", qp)},
	)
	if got := s.ToRecovered(); len(got) != 1 || got[0].RemainQty.String() != "3" {
		t.Fatalf("after partial fill want remain 3, got %+v", got)
	}

	s.Apply(&events.OrderCanceled{Base: base(4), OrderID: "o1", Side: types.Bid,
		CanceledQty: dec("3", qp)})
	if got := s.ToRecovered(); len(got) != 0 {
		t.Fatalf("after cancel want empty, got %+v", got)
	}
}

func TestFold_FullyFilledMakerRemoved(t *testing.T) {
	s := foldAll(
		&events.OrderAccepted{Base: base(1), OrderID: "o1", UserID: "alice", Side: types.Ask,
			OrderType: types.Limit, Price: dec("101.00", pp), OrigQty: dec("4", qp),
			DisplayQty: dec("4", qp), TIF: types.GTC, OrderSeqNum: 1},
		&events.OrderRested{Base: base(2), OrderID: "o1", Side: types.Ask,
			Price: dec("101.00", pp), RemainQty: dec("4", qp), DisplayQty: dec("4", qp)},
		&events.TradeFill{Base: base(3), OrderID: "o1", UserID: "alice", Role: events.RoleMaker,
			Side: types.Ask, Price: dec("101.00", pp), FilledQty: dec("4", qp), RemainQty: dec("0", qp)},
	)
	if got := s.ToRecovered(); len(got) != 0 {
		t.Fatalf("fully filled maker should be gone, got %+v", got)
	}
}

func TestFold_TakerNeverRests(t *testing.T) {
	// An IOC taker is accepted, fully fills, never rests → must not appear.
	s := foldAll(
		&events.OrderAccepted{Base: base(1), OrderID: "taker", UserID: "bob", Side: types.Bid,
			OrderType: types.Limit, Price: dec("101.00", pp), OrigQty: dec("4", qp),
			DisplayQty: dec("4", qp), TIF: types.IOC, OrderSeqNum: 1},
		&events.TradeFill{Base: base(2), OrderID: "taker", UserID: "bob", Role: events.RoleTaker,
			Side: types.Bid, Price: dec("101.00", pp), FilledQty: dec("4", qp), RemainQty: dec("0", qp)},
	)
	if got := s.ToRecovered(); len(got) != 0 {
		t.Fatalf("fully-filled taker must not rest, got %+v", got)
	}
}

func TestFold_StopPendingThenTriggeredToMarket(t *testing.T) {
	// A stop-market rests in the stop book, then triggers and converts to a market
	// order (re-accepted with same OrderID, OrderType=Market) that fully fills.
	// It must not linger in the recovered set.
	s := foldAll(
		&events.OrderAccepted{Base: base(1), OrderID: "s1", UserID: "carol", Side: types.Ask,
			OrderType: types.Market, StopPrice: dec("90.00", pp), Price: dec("0.00", pp),
			OrigQty: dec("2", qp), DisplayQty: dec("2", qp), TIF: types.GTC, OrderSeqNum: 1},
	)
	if got := s.ToRecovered(); len(got) != 1 || got[0].Type != types.Stop {
		t.Fatalf("pending stop should be present as Stop, got %+v", got)
	}

	// Trigger + convert to market, then fully fill.
	s.Apply(&events.StopTriggered{Base: base(2), StopOrderID: "s1", UserID: "carol"})
	s.Apply(&events.OrderAccepted{Base: base(3), OrderID: "s1", UserID: "carol", Side: types.Ask,
		OrderType: types.Market, OrigQty: dec("2", qp), DisplayQty: dec("2", qp), OrderSeqNum: 2})
	s.Apply(&events.TradeFill{Base: base(4), OrderID: "s1", UserID: "carol", Role: events.RoleTaker,
		Side: types.Ask, Price: dec("89.00", pp), FilledQty: dec("2", qp), RemainQty: dec("0", qp)})

	if got := s.ToRecovered(); len(got) != 0 {
		t.Fatalf("triggered+filled stop must be gone, got %+v", got)
	}
}

func TestFold_IdempotentReplay(t *testing.T) {
	accept := &events.OrderAccepted{Base: base(1), OrderID: "o1", UserID: "alice", Side: types.Bid,
		OrderType: types.Limit, Price: dec("100.00", pp), OrigQty: dec("5", qp),
		DisplayQty: dec("5", qp), TIF: types.GTC, OrderSeqNum: 1}
	rested := &events.OrderRested{Base: base(2), OrderID: "o1", Side: types.Bid,
		Price: dec("100.00", pp), RemainQty: dec("5", qp), DisplayQty: dec("5", qp)}

	s := foldAll(accept, rested)
	// Re-apply the same events (seq <= LastEventSeq) — must be no-ops.
	s.Apply(accept)
	s.Apply(rested)

	if got := s.ToRecovered(); len(got) != 1 || got[0].RemainQty.String() != "5" {
		t.Fatalf("replay should be idempotent, got %+v", got)
	}
	if s.LastEventSeq != 2 {
		t.Errorf("LastEventSeq = %d, want 2", s.LastEventSeq)
	}
}

func TestFold_StateRoundTripsJSON(t *testing.T) {
	// The checkpoint blob is JSON; ensure a folded state survives a round-trip,
	// including fixed-point decimals.
	s := foldAll(
		&events.OrderAccepted{Base: base(1), OrderID: "o1", UserID: "alice", Side: types.Bid,
			OrderType: types.Limit, Price: dec("100.25", pp), OrigQty: dec("5", qp),
			DisplayQty: dec("5", qp), TIF: types.GTC, OrderSeqNum: 1},
		&events.OrderRested{Base: base(2), OrderID: "o1", Side: types.Bid,
			Price: dec("100.25", pp), RemainQty: dec("5", qp), DisplayQty: dec("5", qp)},
	)

	blob, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var s2 BookState
	if err := json.Unmarshal(blob, &s2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := s2.ToRecovered()
	if len(got) != 1 || got[0].Price.String() != "100.25" || got[0].RemainQty.String() != "5" {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
	if s2.LastEventSeq != 2 {
		t.Errorf("LastEventSeq after round-trip = %d, want 2", s2.LastEventSeq)
	}
}
