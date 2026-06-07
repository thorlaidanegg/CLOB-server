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
// PlaceOrderResponse is returned to REST/WS clients; JSON tags are camelCase to
// match the API contract (see api/openapi.yaml).
type PlaceOrderResponse struct {
	OrderID string `json:"orderID"`
	SeqNum  uint64 `json:"seqNum"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
}

// CreateMarketRequest asks the engine to register a (already-persisted) market
// with the running MultiEngine, optionally running an opening call-auction.
type CreateMarketRequest struct {
	MarketID         string
	Auction          bool
	AuctionPreOpenMs int64
	ReferencePrice   string // decimal string; auction tiebreaker
}

// CreateMarketResponse reports the engine state after creation.
type CreateMarketResponse struct {
	Created bool
	State   string // "open" | "auction" | "exists"
}

// MarketStats holds engine resource utilization for a single market.
type MarketStats struct {
	MarketID          string
	State             string
	OrderSeq          uint64
	EventSeq          uint64
	OpenOrders        int
	StopOrders        int
	BidLevels         int
	AskLevels         int
	NodePoolUsed      int
	NodePoolCapacity  int
	LevelPoolUsed     int
	LevelPoolCapacity int
}

// EngineAdapter is the interface for order routing. Both gRPC and in-process adapters implement it.
type EngineAdapter interface {
	PlaceOrder(ctx context.Context, req PlaceOrderRequest) (PlaceOrderResponse, error)
	CancelOrder(ctx context.Context, orderID, userID string) error
	GetDepth(ctx context.Context, marketID string, levels int) (events.BookSnapshot, error)
	GetBBO(ctx context.Context, marketID string) (bid, ask string, err error)
	GetStats(ctx context.Context, marketID string) (MarketStats, error)
	CreateMarket(ctx context.Context, req CreateMarketRequest) (CreateMarketResponse, error)
}
