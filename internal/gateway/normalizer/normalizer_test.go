package normalizer

import (
	"strings"
	"testing"
	"time"

	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
)

// testMarket returns a market row with price precision 2, qty precision 8.
func testMarket() pgstore.MarketRow {
	return pgstore.MarketRow{
		MarketID:       "BTC-USD",
		PricePrecision: 2,
		QtyPrecision:   8,
		State:          "open",
	}
}

func TestBuildPlaceRequest_LimitOrder(t *testing.T) {
	res, err := BuildPlaceRequest("usr_1", OrderParams{
		MarketID:  "BTC-USD",
		Side:      "bid",
		OrderType: "limit",
		Price:     "100.00",
		Qty:       "2.50000000",
		TIF:       "GTC",
	}, testMarket())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.HasPrefix(res.OrderRow.OrderID, "ord_") {
		t.Errorf("OrderID should start with ord_, got %q", res.OrderRow.OrderID)
	}
	if res.EngineReq.OrderID != res.OrderRow.OrderID {
		t.Errorf("engine req and order row must share OrderID: %q vs %q", res.EngineReq.OrderID, res.OrderRow.OrderID)
	}
	if res.OrderRow.UserID != "usr_1" {
		t.Errorf("UserID = %q, want usr_1", res.OrderRow.UserID)
	}
	// price 100.00 at precision 2 → raw 10000
	if res.OrderRow.Price != 10000 {
		t.Errorf("Price raw = %d, want 10000", res.OrderRow.Price)
	}
	// qty 2.5 at precision 8 → raw 250000000
	if res.OrderRow.OrigQty != 250000000 {
		t.Errorf("OrigQty raw = %d, want 250000000", res.OrderRow.OrigQty)
	}
	if res.OrderRow.RemainQty != res.OrderRow.OrigQty {
		t.Errorf("RemainQty should equal OrigQty on a new order")
	}
	if res.OrderRow.Status != "new" {
		t.Errorf("Status = %q, want new", res.OrderRow.Status)
	}
}

func TestBuildPlaceRequest_MarketOrderNoPrice(t *testing.T) {
	res, err := BuildPlaceRequest("usr_1", OrderParams{
		MarketID:  "BTC-USD",
		Side:      "ask",
		OrderType: "market",
		Qty:       "1.00000000",
	}, testMarket())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.OrderRow.Price != 0 {
		t.Errorf("market order should have price 0, got %d", res.OrderRow.Price)
	}
}

func TestBuildPlaceRequest_DefaultsOrderTypeAndTIF(t *testing.T) {
	res, err := BuildPlaceRequest("usr_1", OrderParams{
		MarketID: "BTC-USD",
		Side:     "bid",
		Price:    "100.00",
		Qty:      "1.00000000",
	}, testMarket())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.EngineReq.OrderType != "limit" {
		t.Errorf("orderType should default to limit, got %q", res.EngineReq.OrderType)
	}
	if res.EngineReq.TIF != "GTC" {
		t.Errorf("TIF should default to GTC, got %q", res.EngineReq.TIF)
	}
}

func TestBuildPlaceRequest_Validation(t *testing.T) {
	cases := []struct {
		name   string
		params OrderParams
		errSub string
	}{
		{"empty market", OrderParams{Side: "bid", Qty: "1"}, "marketID"},
		{"bad side", OrderParams{MarketID: "BTC-USD", Side: "buy", Qty: "1", Price: "1.00"}, "side"},
		{"bad tif", OrderParams{MarketID: "BTC-USD", Side: "bid", Qty: "1", Price: "1.00", TIF: "ABC"}, "TIF"},
		{"bad order type", OrderParams{MarketID: "BTC-USD", Side: "bid", Qty: "1", Price: "1.00", OrderType: "twap"}, "orderType"},
		{"missing qty", OrderParams{MarketID: "BTC-USD", Side: "bid", Price: "1.00"}, "qty"},
		{"limit missing price", OrderParams{MarketID: "BTC-USD", Side: "bid", OrderType: "limit", Qty: "1"}, "price is required"},
		{"stop missing stopPrice", OrderParams{MarketID: "BTC-USD", Side: "bid", OrderType: "stop", Qty: "1"}, "stopPrice is required"},
		{"GTD missing expireAt", OrderParams{MarketID: "BTC-USD", Side: "bid", OrderType: "limit", Price: "1.00", Qty: "1", TIF: "GTD"}, "expireAt is required"},
		{"bad price decimal", OrderParams{MarketID: "BTC-USD", Side: "bid", OrderType: "limit", Price: "abc", Qty: "1"}, "invalid price"},
		{"too many price decimals", OrderParams{MarketID: "BTC-USD", Side: "bid", OrderType: "limit", Price: "1.123", Qty: "1"}, "invalid price"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := BuildPlaceRequest("usr_1", tc.params, testMarket())
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errSub)
			}
			if !strings.Contains(err.Error(), tc.errSub) {
				t.Errorf("error = %q, want substring %q", err.Error(), tc.errSub)
			}
		})
	}
}

func TestBuildPlaceRequest_GTDParsesExpiry(t *testing.T) {
	future := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	res, err := BuildPlaceRequest("usr_1", OrderParams{
		MarketID:  "BTC-USD",
		Side:      "bid",
		OrderType: "limit",
		Price:     "100.00",
		Qty:       "1.00000000",
		TIF:       "GTD",
		ExpireAt:  future,
	}, testMarket())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.EngineReq.ExpireAt == 0 {
		t.Error("ExpireAt should be parsed to a non-zero unix ns")
	}
}

func TestBuildPlaceRequest_IcebergDisplayQty(t *testing.T) {
	res, err := BuildPlaceRequest("usr_1", OrderParams{
		MarketID:   "BTC-USD",
		Side:       "bid",
		OrderType:  "iceberg",
		Price:      "100.00",
		Qty:        "10.00000000",
		DisplayQty: "2.00000000",
	}, testMarket())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.OrderRow.DisplayQty != 200000000 {
		t.Errorf("DisplayQty raw = %d, want 200000000", res.OrderRow.DisplayQty)
	}
}

func TestNewOrderID_UniqueAndPrefixed(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := NewOrderID()
		if !strings.HasPrefix(id, "ord_") {
			t.Fatalf("id %q missing ord_ prefix", id)
		}
		if seen[id] {
			t.Fatalf("duplicate order id generated: %q", id)
		}
		seen[id] = true
	}
}
