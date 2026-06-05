package normalizer

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	gatewayclient "github.com/thorlaidanegg/clob-server/internal/gateway/client"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob/types"
)

// OrderParams is the unified input struct for placing an order from any transport
// (REST or WebSocket). Callers populate this from their transport-specific format
// and pass it to BuildPlaceRequest.
type OrderParams struct {
	MarketID   string
	Side       string // "bid" | "ask"
	OrderType  string // "limit" | "market" | "stop" | "stop_limit" | "iceberg"
	Price      string // decimal string; empty for market orders
	StopPrice  string // decimal string; stop orders only
	Qty        string // decimal string
	DisplayQty string // decimal string; iceberg only
	TIF        string // "GTC" | "IOC" | "FOK" | "GTD" | "DAY"; defaults to GTC
	ExpireAt   string // RFC3339; GTD only; empty means no expiry
	STPMode    string // "cancel_both" | "cancel_maker" | "cancel_taker" | "decrement_cancel"
	PostOnly   bool   // reject if the order would take liquidity (maker-only)
	ReduceOnly bool   // reject if the order would increase the position
}

// BuildResult is what both REST and WS handlers need after normalisation.
type BuildResult struct {
	EngineReq gatewayclient.PlaceOrderRequest
	OrderRow  ordersstore.OrderRow
}

// BuildPlaceRequest validates params, generates an OrderID, and returns everything
// both handlers need: a PlaceOrderRequest for the engine and an OrderRow for Postgres.
// mkt is needed to convert decimal strings to BIGINT for the orders table.
func BuildPlaceRequest(userID string, p OrderParams, mkt pgstore.MarketRow) (BuildResult, error) {
	if p.MarketID == "" {
		return BuildResult{}, fmt.Errorf("marketID is required")
	}
	if err := validateSide(p.Side); err != nil {
		return BuildResult{}, err
	}
	if p.OrderType == "" {
		p.OrderType = "limit"
	}
	if err := validateOrderType(p.OrderType); err != nil {
		return BuildResult{}, err
	}
	if p.TIF == "" {
		// Market orders never rest — default them to IOC instead of GTC so they
		// aren't rejected by the engine ("market orders cannot use resting TIF").
		if p.OrderType == "market" {
			p.TIF = "IOC"
		} else {
			p.TIF = "GTC"
		}
	}
	if err := validateTIF(p.TIF); err != nil {
		return BuildResult{}, err
	}
	// Reject a resting TIF on a market order up front (a clear 400) rather than
	// letting the engine reject it asynchronously after a 202.
	if p.OrderType == "market" && (p.TIF == "GTC" || p.TIF == "GTD" || p.TIF == "DAY") {
		return BuildResult{}, fmt.Errorf("market orders must use IOC or FOK, not %s", p.TIF)
	}
	if p.Qty == "" {
		return BuildResult{}, fmt.Errorf("qty is required")
	}

	// Parse qty — required for all order types.
	qty, err := types.ParseDecimal(p.Qty, mkt.QtyPrecision)
	if err != nil {
		return BuildResult{}, fmt.Errorf("invalid qty %q: %w", p.Qty, err)
	}

	// Parse price — required for limit / stop_limit / iceberg.
	var priceInt int64
	if p.OrderType == "limit" || p.OrderType == "iceberg" || p.OrderType == "stop_limit" {
		if p.Price == "" {
			return BuildResult{}, fmt.Errorf("price is required for %s orders", p.OrderType)
		}
		price, err := types.ParseDecimal(p.Price, mkt.PricePrecision)
		if err != nil {
			return BuildResult{}, fmt.Errorf("invalid price %q: %w", p.Price, err)
		}
		priceInt = price.Value()
	}

	// Parse stop_price — required for stop / stop_limit.
	var stopPriceInt int64
	if p.OrderType == "stop" || p.OrderType == "stop_limit" {
		if p.StopPrice == "" {
			return BuildResult{}, fmt.Errorf("stopPrice is required for %s orders", p.OrderType)
		}
		sp, err := types.ParseDecimal(p.StopPrice, mkt.PricePrecision)
		if err != nil {
			return BuildResult{}, fmt.Errorf("invalid stopPrice %q: %w", p.StopPrice, err)
		}
		stopPriceInt = sp.Value()
	}

	// Parse display_qty — iceberg only.
	var displayQtyInt int64
	if p.OrderType == "iceberg" && p.DisplayQty != "" {
		dq, err := types.ParseDecimal(p.DisplayQty, mkt.QtyPrecision)
		if err != nil {
			return BuildResult{}, fmt.Errorf("invalid displayQty %q: %w", p.DisplayQty, err)
		}
		displayQtyInt = dq.Value()
	}

	// Parse expiry — GTD only.
	var expireAtNs int64
	if p.TIF == "GTD" {
		if p.ExpireAt == "" {
			return BuildResult{}, fmt.Errorf("expireAt is required for GTD orders")
		}
		t, err := time.Parse(time.RFC3339, p.ExpireAt)
		if err != nil {
			return BuildResult{}, fmt.Errorf("invalid expireAt %q: must be RFC3339", p.ExpireAt)
		}
		expireAtNs = t.UnixNano()
	}

	orderID := NewOrderID()

	var flags []string
	if p.PostOnly {
		flags = append(flags, "post_only")
	}
	if p.ReduceOnly {
		flags = append(flags, "reduce_only")
	}

	engineReq := gatewayclient.PlaceOrderRequest{
		OrderID:    orderID,
		UserID:     userID,
		MarketID:   p.MarketID,
		Side:       p.Side,
		OrderType:  p.OrderType,
		Price:      p.Price,
		StopPrice:  p.StopPrice,
		Qty:        p.Qty,
		DisplayQty: p.DisplayQty,
		TIF:        p.TIF,
		ExpireAt:   expireAtNs,
		STPMode:    p.STPMode,
		Flags:      flags,
	}

	orderRow := ordersstore.OrderRow{
		OrderID:    orderID,
		UserID:     userID,
		MarketID:   p.MarketID,
		Side:       p.Side,
		OrderType:  p.OrderType,
		Price:      priceInt,
		StopPrice:  stopPriceInt,
		OrigQty:    qty.Value(),
		RemainQty:  qty.Value(),
		DisplayQty: displayQtyInt,
		Status:     "new",
		TIF:        p.TIF,
		Flags:      orderRowFlags(p),
	}

	return BuildResult{EngineReq: engineReq, OrderRow: orderRow}, nil
}

// orderRowFlags mirrors the engine's OrderFlags bitfield for the stored row
// (post_only = 1<<0, reduce_only = 1<<1) so the persisted order reflects intent.
func orderRowFlags(p OrderParams) int {
	var f int
	if p.PostOnly {
		f |= 1
	}
	if p.ReduceOnly {
		f |= 2
	}
	return f
}

// NewOrderID generates a unique order ID. Format: ord_<16 random hex chars>.
// Guaranteed unique across restarts; never derived from client input.
func NewOrderID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		// Fallback to timestamp if crypto/rand fails (should never happen).
		return fmt.Sprintf("ord_%d", time.Now().UnixNano())
	}
	return "ord_" + hex.EncodeToString(b)
}

// ValidSides and ValidTIFs are the accepted string values.
var validSides = map[string]bool{"bid": true, "ask": true}

var validTIFs = map[string]bool{
	"GTC": true, "IOC": true, "FOK": true, "GTD": true, "DAY": true,
}

var validOrderTypes = map[string]bool{
	"limit": true, "market": true, "stop": true, "stop_limit": true, "iceberg": true,
}

func validateSide(s string) error {
	if !validSides[s] {
		return fmt.Errorf("invalid side %q: must be bid or ask", s)
	}
	return nil
}

func validateTIF(s string) error {
	if !validTIFs[s] {
		return fmt.Errorf("invalid TIF %q: must be GTC, IOC, FOK, GTD or DAY", s)
	}
	return nil
}

func validateOrderType(s string) error {
	if !validOrderTypes[s] {
		return fmt.Errorf("invalid orderType %q: must be limit, market, stop, stop_limit or iceberg", s)
	}
	return nil
}
