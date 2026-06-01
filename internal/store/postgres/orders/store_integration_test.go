package orders_test

import (
	"context"
	"testing"

	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
)

func TestOrderStore_InsertGetUpdate(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	store := ordersstore.NewPgStore(pool)
	ctx := context.Background()

	row := ordersstore.OrderRow{
		OrderID:   "ord_1",
		UserID:    "alice",
		MarketID:  "BTC-USD",
		Side:      "bid",
		OrderType: "limit",
		Price:     10000,
		OrigQty:   500,
		RemainQty: 500,
		Status:    "new",
		TIF:       "GTC",
	}
	if err := store.InsertOrder(ctx, row); err != nil {
		t.Fatalf("insert: %v", err)
	}

	got, err := store.GetOrder(ctx, "ord_1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.UserID != "alice" || got.Price != 10000 || got.RemainQty != 500 {
		t.Errorf("unexpected row: %+v", got)
	}
	if got.ReservedPerUnit != 0 {
		t.Errorf("reserved_per_unit should default to 0, got %d", got.ReservedPerUnit)
	}

	// UpdateReservedPerUnit (the hook's write-back).
	if err := store.UpdateReservedPerUnit(ctx, "ord_1", 20000); err != nil {
		t.Fatalf("update reserved_per_unit: %v", err)
	}
	got, _ = store.GetOrder(ctx, "ord_1")
	if got.ReservedPerUnit != 20000 {
		t.Errorf("reserved_per_unit = %d, want 20000", got.ReservedPerUnit)
	}

	// UpdateOrderStatus.
	if err := store.UpdateOrderStatus(ctx, "ord_1", "rested"); err != nil {
		t.Fatalf("update status: %v", err)
	}
	got, _ = store.GetOrder(ctx, "ord_1")
	if got.Status != "rested" {
		t.Errorf("status = %q, want rested", got.Status)
	}
}

func TestOrderStore_ListOpenOrders(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	store := ordersstore.NewPgStore(pool)
	ctx := context.Background()

	mk := func(id, status string) ordersstore.OrderRow {
		return ordersstore.OrderRow{
			OrderID: id, UserID: "alice", MarketID: "BTC-USD", Side: "bid",
			OrderType: "limit", Price: 10000, OrigQty: 100, RemainQty: 100,
			Status: status, TIF: "GTC",
		}
	}
	store.InsertOrder(ctx, mk("ord_open1", "rested"))
	store.InsertOrder(ctx, mk("ord_open2", "new"))
	store.InsertOrder(ctx, mk("ord_done", "filled"))
	store.InsertOrder(ctx, ordersstore.OrderRow{
		OrderID: "ord_other", UserID: "bob", MarketID: "BTC-USD", Side: "bid",
		OrderType: "limit", Price: 10000, OrigQty: 100, RemainQty: 100, Status: "rested", TIF: "GTC",
	})

	open, err := store.ListOpenOrders(ctx, "alice")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(open) != 2 {
		t.Errorf("expected 2 open orders for alice, got %d", len(open))
	}
	for _, o := range open {
		if o.Status == "filled" {
			t.Error("filled order should not be in open list")
		}
		if o.UserID != "alice" {
			t.Error("another user's order leaked into the list")
		}
	}
}
