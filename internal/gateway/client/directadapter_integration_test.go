package client_test

import (
	"context"
	"sync"
	"testing"
	"time"

	gatewayclient "github.com/thorlaidanegg/clob-server/internal/gateway/client"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/testutil"
)

// fakeOrderStore is an in-memory ordersstore.Store for testing the cancel path,
// which needs to resolve a marketID from an orderID.
type fakeOrderStore struct {
	mu     sync.Mutex
	orders map[string]ordersstore.OrderRow
}

func newFakeOrderStore() *fakeOrderStore {
	return &fakeOrderStore{orders: make(map[string]ordersstore.OrderRow)}
}

func (f *fakeOrderStore) InsertOrder(_ context.Context, o ordersstore.OrderRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.orders[o.OrderID] = o
	return nil
}

func (f *fakeOrderStore) GetOrder(_ context.Context, id string) (ordersstore.OrderRow, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.orders[id]
	if !ok {
		return ordersstore.OrderRow{}, context.Canceled // any non-nil error
	}
	return o, nil
}

func (f *fakeOrderStore) UpdateOrderStatus(_ context.Context, _, _ string) error      { return nil }
func (f *fakeOrderStore) UpdateReservedPerUnit(_ context.Context, _ string, _ int64) error { return nil }
func (f *fakeOrderStore) ListOpenOrders(_ context.Context, _ string) ([]ordersstore.OrderRow, error) {
	return nil, nil
}

// setupEngine builds a real in-memory MultiEngine with one market and a direct adapter.
func setupEngine(t *testing.T) (gatewayclient.EngineAdapter, *fakeOrderStore, *engine.MultiEngine) {
	t.Helper()
	cfg := testutil.DefaultConfig("BTC-USD")
	multi := engine.NewMultiEngine()
	if err := multi.CreateMarket(cfg); err != nil {
		t.Fatalf("create market: %v", err)
	}
	t.Cleanup(func() { multi.Close() })

	// Markets start in PreOpen; resume to enter continuous trading.
	if err := multi.Submit(engine.AdminResumeMarket{MarketID: cfg.MarketID}); err != nil {
		t.Fatalf("resume market: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	store := newFakeOrderStore()
	adapter := gatewayclient.NewDirectAdapter(multi, []clobconfig.MarketConfig{cfg}, store, nil)
	return adapter, store, multi
}

// waitForBBO polls GetBBO until the bid matches want or the deadline passes.
func waitForBid(t *testing.T, a gatewayclient.EngineAdapter, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bid, _, err := a.GetBBO(context.Background(), "BTC-USD")
		if err == nil && bid == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for best bid = %q", want)
}

func TestDirectAdapter_PlaceLimitOrderRestsInBook(t *testing.T) {
	adapter, _, _ := setupEngine(t)

	resp, err := adapter.PlaceOrder(context.Background(), gatewayclient.PlaceOrderRequest{
		OrderID:   "ord_1",
		UserID:    "alice",
		MarketID:  "BTC-USD",
		Side:      "bid",
		OrderType: "limit",
		Price:     "100.00",
		Qty:       "5",
		TIF:       "GTC",
	})
	if err != nil {
		t.Fatalf("place order: %v", err)
	}
	if resp.Status != "accepted" {
		t.Errorf("status = %q, want accepted", resp.Status)
	}

	waitForBid(t, adapter, "100.00")

	snap, err := adapter.GetDepth(context.Background(), "BTC-USD", 10)
	if err != nil {
		t.Fatalf("get depth: %v", err)
	}
	if len(snap.Bids) != 1 {
		t.Fatalf("expected 1 bid level, got %d", len(snap.Bids))
	}
	if snap.Bids[0].Price.String() != "100.00" {
		t.Errorf("bid price = %q, want 100.00", snap.Bids[0].Price.String())
	}
	if snap.Bids[0].TotalQty.String() != "5" {
		t.Errorf("bid qty = %q, want 5", snap.Bids[0].TotalQty.String())
	}
}

func TestDirectAdapter_CrossingOrdersFill(t *testing.T) {
	adapter, _, multi := setupEngine(t)

	// Subscribe to events to confirm a trade occurs.
	evCh, err := multi.Events("BTC-USD")
	if err != nil {
		t.Fatalf("events: %v", err)
	}

	// Resting bid.
	_, err = adapter.PlaceOrder(context.Background(), gatewayclient.PlaceOrderRequest{
		OrderID: "ord_bid", UserID: "alice", MarketID: "BTC-USD",
		Side: "bid", OrderType: "limit", Price: "100.00", Qty: "5", TIF: "GTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForBid(t, adapter, "100.00")

	// Crossing ask.
	_, err = adapter.PlaceOrder(context.Background(), gatewayclient.PlaceOrderRequest{
		OrderID: "ord_ask", UserID: "bob", MarketID: "BTC-USD",
		Side: "ask", OrderType: "limit", Price: "99.00", Qty: "5", TIF: "GTC",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Drain events looking for a TradeExecuted within a timeout.
	sawTrade := false
	timeout := time.After(2 * time.Second)
	for !sawTrade {
		select {
		case ev := <-evCh:
			if ev.Type() == "trade_executed" {
				sawTrade = true
			}
		case <-timeout:
			t.Fatal("no trade_executed event after crossing orders")
		}
	}

	// Book should be empty after a full fill.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bid, ask, _ := adapter.GetBBO(context.Background(), "BTC-USD")
		if bid == "" && ask == "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("book should be empty after a full fill")
}

func TestDirectAdapter_GetStats(t *testing.T) {
	adapter, _, _ := setupEngine(t)

	_, err := adapter.PlaceOrder(context.Background(), gatewayclient.PlaceOrderRequest{
		OrderID: "ord_s", UserID: "alice", MarketID: "BTC-USD",
		Side: "bid", OrderType: "limit", Price: "100.00", Qty: "5", TIF: "GTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForBid(t, adapter, "100.00")

	stats, err := adapter.GetStats(context.Background(), "BTC-USD")
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	if stats.MarketID != "BTC-USD" {
		t.Errorf("marketID = %q, want BTC-USD", stats.MarketID)
	}
	if stats.OpenOrders != 1 {
		t.Errorf("openOrders = %d, want 1", stats.OpenOrders)
	}
	if stats.BidLevels != 1 {
		t.Errorf("bidLevels = %d, want 1", stats.BidLevels)
	}
	if stats.NodePoolCapacity == 0 {
		t.Error("nodePoolCapacity should be reported (> 0)")
	}
}

func TestDirectAdapter_GetStatsUnknownMarket(t *testing.T) {
	adapter, _, _ := setupEngine(t)
	if _, err := adapter.GetStats(context.Background(), "NOPE"); err == nil {
		t.Error("expected error for unknown market")
	}
}

func TestDirectAdapter_MarketNotFound(t *testing.T) {
	adapter, _, _ := setupEngine(t)

	_, err := adapter.PlaceOrder(context.Background(), gatewayclient.PlaceOrderRequest{
		OrderID: "ord_x", UserID: "alice", MarketID: "NOPE",
		Side: "bid", OrderType: "limit", Price: "1.00", Qty: "1", TIF: "GTC",
	})
	if err == nil {
		t.Error("expected error placing order on unknown market")
	}
}

func TestDirectAdapter_CancelResolvesMarketFromStore(t *testing.T) {
	adapter, store, _ := setupEngine(t)

	// Mimic the handler: insert the order row first, then place.
	store.InsertOrder(context.Background(), ordersstore.OrderRow{
		OrderID: "ord_c", UserID: "alice", MarketID: "BTC-USD", Side: "bid", Status: "new",
	})
	_, err := adapter.PlaceOrder(context.Background(), gatewayclient.PlaceOrderRequest{
		OrderID: "ord_c", UserID: "alice", MarketID: "BTC-USD",
		Side: "bid", OrderType: "limit", Price: "100.00", Qty: "5", TIF: "GTC",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForBid(t, adapter, "100.00")

	// Cancel resolves marketID from the store and submits to the engine.
	if err := adapter.CancelOrder(context.Background(), "ord_c", "alice"); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bid, _, _ := adapter.GetBBO(context.Background(), "BTC-USD")
		if bid == "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("bid should be gone after cancel")
}

func TestDirectAdapter_CancelUnknownOrder(t *testing.T) {
	adapter, _, _ := setupEngine(t)
	if err := adapter.CancelOrder(context.Background(), "ord_missing", "alice"); err == nil {
		t.Error("cancel of unknown order should error (cannot resolve market)")
	}
}
