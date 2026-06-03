// Package bookstate reconstructs an engine's resting order book by folding the
// market-events log. It is the heart of crash recovery: the engine's own event
// stream is the source of truth, and replaying it rebuilds exactly which orders
// are still resting and at what remaining quantity.
//
// The fold is a pure function of the event sequence — no database, no Kafka — so
// recovery correctness is proven by fast unit tests over hand-built event slices.
package bookstate

import (
	"sort"

	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
)

// RestingOrder is one order currently resting in the reconstructed book, plus the
// engine sequence number that fixes its time priority.
type RestingOrder struct {
	Order  engine.RecoveredOrder `json:"order"`
	SeqNum uint64                `json:"seqNum"`
}

// BookState is the folded resting set for a single market at a point in the log.
// It is serializable (used as the checkpoint blob) and replay-idempotent: events
// at or below LastEventSeq are ignored.
type BookState struct {
	Orders       map[types.OrderID]RestingOrder `json:"orders"`
	LastEventSeq uint64                         `json:"lastEventSeq"`
}

// New returns an empty BookState ready to fold events into.
func New() *BookState {
	return &BookState{Orders: make(map[types.OrderID]RestingOrder)}
}

// Apply folds a single event into the state. Events with SeqNum <= LastEventSeq
// are skipped, so replaying an overlapping log range is safe. ev should be a
// pointer to a concrete event type (as produced by the worker deserializer).
func (s *BookState) Apply(ev events.Event) {
	seq := ev.SeqNum()
	if seq != 0 && seq <= s.LastEventSeq {
		return
	}

	switch e := ev.(type) {
	case *events.OrderAccepted:
		s.applyAccepted(e)
	case *events.OrderRested:
		if ro, ok := s.Orders[e.OrderID]; ok {
			ro.Order.RemainQty = e.RemainQty
			ro.Order.DisplayQty = e.DisplayQty
			s.Orders[e.OrderID] = ro
		}
	case *events.TradeFill:
		if ro, ok := s.Orders[e.OrderID]; ok {
			if e.RemainQty.IsPositive() {
				ro.Order.RemainQty = e.RemainQty
				s.Orders[e.OrderID] = ro
			} else {
				delete(s.Orders, e.OrderID)
			}
		}
	case *events.OrderCanceled:
		delete(s.Orders, e.OrderID)
	case *events.OrderExpired:
		delete(s.Orders, e.OrderID)
	case *events.OrderRejected:
		delete(s.Orders, e.OrderID)
	}
	// TradeExecuted, DepthUpdate, StopTriggered, market state, auction events
	// carry no resting-set change beyond what the above already capture.

	if seq > s.LastEventSeq {
		s.LastEventSeq = seq
	}
}

func (s *BookState) applyAccepted(e *events.OrderAccepted) {
	switch {
	case e.StopPrice.IsPositive():
		// Pending stop in the stop book. OrderType is what it converts to:
		// Limit => stop-limit, anything else => stop-market.
		typ := types.Stop
		if e.OrderType == types.Limit {
			typ = types.StopLimit
		}
		s.Orders[e.OrderID] = RestingOrder{
			SeqNum: e.OrderSeqNum,
			Order: engine.RecoveredOrder{
				OrderID: e.OrderID, UserID: e.UserID, Side: e.Side, Type: typ,
				Price: e.Price, StopPrice: e.StopPrice,
				RemainQty: e.OrigQty, DisplayQty: e.OrigQty,
				Flags: e.Flags, TIF: e.TIF,
			},
		}
	case e.OrderType == types.Market:
		// Plain market orders never rest. This also clears the stale stop entry
		// when a triggered stop is re-accepted as a market order (same OrderID).
		delete(s.Orders, e.OrderID)
	default:
		// Limit / iceberg. Added optimistically; an OrderRested confirms the
		// resting remainder, fills reduce it, and terminal events remove it.
		s.Orders[e.OrderID] = RestingOrder{
			SeqNum: e.OrderSeqNum,
			Order: engine.RecoveredOrder{
				OrderID: e.OrderID, UserID: e.UserID, Side: e.Side, Type: types.Limit,
				Price: e.Price, RemainQty: e.OrigQty, DisplayQty: e.DisplayQty,
				Flags: e.Flags, TIF: e.TIF,
			},
		}
	}
}

// ToRecovered returns the resting orders in time-priority order (ascending
// engine sequence number), ready to pass to engine.WithInitialOrders.
func (s *BookState) ToRecovered() []engine.RecoveredOrder {
	rs := make([]RestingOrder, 0, len(s.Orders))
	for _, ro := range s.Orders {
		rs = append(rs, ro)
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].SeqNum < rs[j].SeqNum })

	out := make([]engine.RecoveredOrder, len(rs))
	for i, ro := range rs {
		out[i] = ro.Order
	}
	return out
}
