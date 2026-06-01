package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	enginev1 "github.com/thorlaidanegg/clob-server/proto/engine/v1"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// EngineClient implements EngineAdapter over gRPC. Used by ROLE=gateway.
type EngineClient struct {
	conn   *grpc.ClientConn
	client enginev1.EngineServiceClient
}

// NewEngineClient dials the engine gRPC server.
// If tlsCAFile is set, the connection uses TLS verifying the server against that
// CA. Otherwise the connection is plaintext (acceptable inside a trusted VPC).
func NewEngineClient(addr, tlsCAFile string) (*EngineClient, error) {
	var creds credentials.TransportCredentials
	if tlsCAFile != "" {
		c, err := credentials.NewClientTLSFromFile(tlsCAFile, "")
		if err != nil {
			return nil, fmt.Errorf("engine client: load TLS CA %s: %w", tlsCAFile, err)
		}
		creds = c
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
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
	for i, b := range resp.Bids {
		snap.Bids[i] = events.DepthLevel{
			Price:      parseDepthDecimal(b.Price),
			TotalQty:   parseDepthDecimal(b.TotalQty),
			DisplayQty: parseDepthDecimal(b.DisplayQty),
			OrderCount: int(b.OrderCount),
		}
	}
	for i, a := range resp.Asks {
		snap.Asks[i] = events.DepthLevel{
			Price:      parseDepthDecimal(a.Price),
			TotalQty:   parseDepthDecimal(a.TotalQty),
			DisplayQty: parseDepthDecimal(a.DisplayQty),
			OrderCount: int(a.OrderCount),
		}
	}
	return snap, nil
}

// parseDepthDecimal converts a decimal string from the engine back into types.Decimal.
//
// The engine produces these strings via Decimal.String(), which always pads the
// fractional part to exactly the value's precision. So the number of digits after
// the decimal point equals the precision — we derive it from the string itself and
// round-trip the value exactly, with no need to know the market's configured precision.
func parseDepthDecimal(s string) types.Decimal {
	if s == "" {
		return types.Decimal{}
	}
	precision := 0
	if dot := strings.IndexByte(s, '.'); dot >= 0 {
		precision = len(s) - dot - 1
	}
	if precision > 18 {
		// Beyond Decimal's supported range — shouldn't happen for engine output.
		precision = 18
	}
	d, err := types.ParseDecimal(s, uint8(precision))
	if err != nil {
		return types.Decimal{}
	}
	return d
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

func (c *EngineClient) GetStats(ctx context.Context, marketID string) (MarketStats, error) {
	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	resp, err := c.client.GetStats(ctx, &enginev1.GetStatsRequest{MarketId: marketID})
	if err != nil {
		return MarketStats{}, err
	}
	return MarketStats{
		MarketID:          resp.MarketId,
		State:             resp.State,
		OrderSeq:          resp.OrderSeq,
		EventSeq:          resp.EventSeq,
		OpenOrders:        int(resp.OpenOrders),
		StopOrders:        int(resp.StopOrders),
		BidLevels:         int(resp.BidLevels),
		AskLevels:         int(resp.AskLevels),
		NodePoolUsed:      int(resp.NodePoolUsed),
		NodePoolCapacity:  int(resp.NodePoolCapacity),
		LevelPoolUsed:     int(resp.LevelPoolUsed),
		LevelPoolCapacity: int(resp.LevelPoolCapacity),
	}, nil
}

func (c *EngineClient) Close() error {
	return c.conn.Close()
}
