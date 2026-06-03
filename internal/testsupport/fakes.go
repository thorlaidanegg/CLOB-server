package testsupport

import (
	"context"
	"sync"

	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob/events"
)

// FakeEngine is an in-memory client.EngineAdapter for handler tests that must not
// reach a real engine. It records calls and returns canned responses, both
// overridable per test.
type FakeEngine struct {
	mu sync.Mutex

	PlaceResp  client.PlaceOrderResponse
	PlaceErr   error
	CancelErr  error
	Depth      events.BookSnapshot
	BBOBid     string
	BBOAsk     string
	Stats      client.MarketStats

	Placed    []client.PlaceOrderRequest
	Canceled  []string
}

var _ client.EngineAdapter = (*FakeEngine)(nil)

func (f *FakeEngine) PlaceOrder(_ context.Context, req client.PlaceOrderRequest) (client.PlaceOrderResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Placed = append(f.Placed, req)
	if f.PlaceErr != nil {
		return client.PlaceOrderResponse{}, f.PlaceErr
	}
	resp := f.PlaceResp
	if resp.OrderID == "" {
		resp.OrderID = req.OrderID
	}
	if resp.Status == "" {
		resp.Status = "accepted"
	}
	return resp, nil
}

func (f *FakeEngine) CancelOrder(_ context.Context, orderID, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Canceled = append(f.Canceled, orderID)
	return f.CancelErr
}

func (f *FakeEngine) GetDepth(_ context.Context, _ string, _ int) (events.BookSnapshot, error) {
	return f.Depth, nil
}

func (f *FakeEngine) GetBBO(_ context.Context, _ string) (string, string, error) {
	return f.BBOBid, f.BBOAsk, nil
}

func (f *FakeEngine) GetStats(_ context.Context, _ string) (client.MarketStats, error) {
	return f.Stats, nil
}

// PlacedCount returns how many PlaceOrder calls were recorded.
func (f *FakeEngine) PlacedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Placed)
}

// CanceledCount returns how many CancelOrder calls were recorded.
func (f *FakeEngine) CanceledCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Canceled)
}
