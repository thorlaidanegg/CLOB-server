package client

import (
	"context"

	"github.com/thorlaidanegg/clob/events"
)

// PlaceOrderRequest carries all parameters for placing an order.
type PlaceOrderRequest struct {
	OrderID    string
	UserID     string
	MarketID   string
	Side       string   // "bid" | "ask"
	OrderType  string   // "limit" | "market" | "stop" | "stop_limit"
	Price      string   // decimal string; empty for market orders
	StopPrice  string   // decimal string; stop orders only
	Qty        string   // decimal string
	DisplayQty string   // decimal string; iceberg only
	TIF        string
	ExpireAt   int64    // unix ns; GTD only
	Flags      []string
	STPMode    string
}

// PlaceOrderResponse is returned after submitting an order.
type PlaceOrderResponse struct {
	OrderID string
	SeqNum  uint64
	Status  string
	Reason  string
}

// EngineAdapter is the interface for order routing. Both gRPC and in-process adapters implement it.
type EngineAdapter interface {
	PlaceOrder(ctx context.Context, req PlaceOrderRequest) (PlaceOrderResponse, error)
	CancelOrder(ctx context.Context, orderID, userID string) error
	GetDepth(ctx context.Context, marketID string, levels int) (events.BookSnapshot, error)
	GetBBO(ctx context.Context, marketID string) (bid, ask string, err error)
}
