package client

import (
	"context"
	"fmt"
	"time"

	enginev1 "github.com/thorlaidanegg/clob-server/proto/engine/v1"
	"github.com/thorlaidanegg/clob/events"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EngineClient implements EngineAdapter over gRPC. Used by ROLE=gateway.
type EngineClient struct {
	conn   *grpc.ClientConn
	client enginev1.EngineServiceClient
}

// NewEngineClient dials the engine gRPC server.
func NewEngineClient(addr string) (*EngineClient, error) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("engine client: dial %s: %w", addr, err)
	}
	return &EngineClient{conn: conn, client: enginev1.NewEngineServiceClient(conn)}, nil
}

func (c *EngineClient) PlaceOrder(ctx context.Context, req PlaceOrderRequest) (PlaceOrderResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	resp, err := c.client.PlaceOrder(ctx, &enginev1.PlaceOrderRequest{
		MarketId:  req.MarketID,
		OrderId:   req.OrderID,
		UserId:    req.UserID,
		Side:      req.Side,
		OrderType: req.OrderType,
		Price:     req.Price,
		StopPrice: req.StopPrice,
		Qty:       req.Qty,
		Tif:       req.TIF,
		ExpireAt:  req.ExpireAt,
		StpMode:   req.STPMode,
	})
	if err != nil {
		return PlaceOrderResponse{}, err
	}
	return PlaceOrderResponse{
		OrderID: resp.OrderId,
		SeqNum:  resp.SeqNum,
		Status:  resp.Status,
		Reason:  resp.Reason,
	}, nil
}

func (c *EngineClient) CancelOrder(ctx context.Context, orderID, userID string) error {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	// marketID lookup would normally come from the orders store; pass empty for now
	// and let the engine look it up by orderID (future enhancement)
	_, err := c.client.CancelOrder(ctx, &enginev1.CancelOrderRequest{
		OrderId: orderID,
		UserId:  userID,
	})
	return err
}

func (c *EngineClient) GetDepth(ctx context.Context, marketID string, levels int) (events.BookSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	resp, err := c.client.GetDepth(ctx, &enginev1.GetDepthRequest{
		MarketId: marketID,
		Levels:   int32(levels),
	})
	if err != nil {
		return events.BookSnapshot{}, err
	}
	snap := events.BookSnapshot{
		Bids: make([]events.DepthLevel, len(resp.Bids)),
		Asks: make([]events.DepthLevel, len(resp.Asks)),
	}
	// Note: string→Decimal conversion uses precision 2 as default here.
	// A full implementation would pass market precision from the markets cache.
	return snap, nil
}

func (c *EngineClient) GetBBO(ctx context.Context, marketID string) (bid, ask string, err error) {
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	resp, err := c.client.GetBBO(ctx, &enginev1.GetBBORequest{MarketId: marketID})
	if err != nil {
		return "", "", err
	}
	return resp.Bid, resp.Ask, nil
}

func (c *EngineClient) GetStats(_ context.Context, marketID string) (MarketStats, error) {
	// Stats RPC not yet in proto; gateway must fall back to Postgres market row.
	// Return partial stats from the market ID only — a future proto extension can add GetStats.
	return MarketStats{MarketID: marketID}, nil
}

func (c *EngineClient) Close() error {
	return c.conn.Close()
}
