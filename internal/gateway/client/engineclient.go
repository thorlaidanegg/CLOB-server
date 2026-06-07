package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	enginev1 "github.com/thorlaidanegg/clob-server/proto/engine/v1"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// engineCallTimeout bounds a single engine RPC. It must comfortably exceed a
// cold connection setup (DNS + TCP + HTTP/2) so the first call after a dial or
// an engine restart doesn't spuriously DeadlineExceed; the happy path still
// returns in well under a millisecond.
const engineCallTimeout = 2 * time.Second

// retryServiceConfig retries transient "Unavailable" RPCs up to 3 times with
// exponential backoff (applied by gRPC itself), per SERVER_LLD §5.
const retryServiceConfig = `{
  "methodConfig": [{
    "name": [{"service": "engine.v1.EngineService"}],
    "retryPolicy": {
      "MaxAttempts": 3,
      "InitialBackoff": "0.05s",
      "MaxBackoff": "0.5s",
      "BackoffMultiplier": 2.0,
      "RetryableStatusCodes": ["UNAVAILABLE"]
    }
  }]
}`

// EngineClient implements EngineAdapter over gRPC. Used by ROLE=gateway.
type EngineClient struct {
	conn       *grpc.ClientConn
	client     enginev1.EngineServiceClient
	orderStore ordersstore.Store
	breaker    *circuitBreaker
}

// NewEngineClient dials the engine gRPC server.
// If tlsCAFile is set, the connection uses TLS verifying the server against that
// CA. Otherwise the connection is plaintext (acceptable inside a trusted VPC).
// orderStore resolves an order's marketID for cancels (the engine routes by market).
func NewEngineClient(addr, tlsCAFile string, orderStore ordersstore.Store) (*EngineClient, error) {
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

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(creds),
		grpc.WithDefaultServiceConfig(retryServiceConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("engine client: dial %s: %w", addr, err)
	}

	// grpc.NewClient connects lazily — the first RPC would otherwise pay the full
	// connection-setup cost and blow its (tight) deadline. Trigger the dial now
	// and best-effort wait for READY so the first real order is served warm.
	conn.Connect()
	warmCtx, cancelWarm := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWarm()
	for {
		s := conn.GetState()
		if s == connectivity.Ready {
			break
		}
		if !conn.WaitForStateChange(warmCtx, s) {
			break // not ready within the warm-up budget; lazy reconnect will cover it
		}
	}

	return &EngineClient{
		conn:       conn,
		client:     enginev1.NewEngineServiceClient(conn),
		orderStore: orderStore,
		breaker:    newCircuitBreaker(),
	}, nil
}

func (c *EngineClient) PlaceOrder(ctx context.Context, req PlaceOrderRequest) (PlaceOrderResponse, error) {
	if !c.breaker.allow() {
		return PlaceOrderResponse{}, apierrors.ErrEngineUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, engineCallTimeout)
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
		Flags:     req.Flags,
	})
	c.breaker.record(err)
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
	// The engine routes commands by marketID, so resolve it from the order record.
	order, err := c.orderStore.GetOrder(ctx, orderID)
	if err != nil {
		return fmt.Errorf("cancel: order not found: %s", orderID)
	}
	if !c.breaker.allow() {
		return apierrors.ErrEngineUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, engineCallTimeout)
	defer cancel()
	_, err = c.client.CancelOrder(ctx, &enginev1.CancelOrderRequest{
		MarketId: order.MarketID,
		OrderId:  orderID,
		UserId:   userID,
	})
	c.breaker.record(err)
	return err
}

func (c *EngineClient) GetDepth(ctx context.Context, marketID string, levels int) (events.BookSnapshot, error) {
	if !c.breaker.allow() {
		return events.BookSnapshot{}, apierrors.ErrEngineUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, engineCallTimeout)
	defer cancel()
	resp, err := c.client.GetDepth(ctx, &enginev1.GetDepthRequest{
		MarketId: marketID,
		Levels:   int32(levels),
	})
	c.breaker.record(err)
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
	if !c.breaker.allow() {
		return "", "", apierrors.ErrEngineUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, engineCallTimeout)
	defer cancel()
	resp, err := c.client.GetBBO(ctx, &enginev1.GetBBORequest{MarketId: marketID})
	c.breaker.record(err)
	if err != nil {
		return "", "", err
	}
	return resp.Bid, resp.Ask, nil
}

func (c *EngineClient) CreateMarket(ctx context.Context, req CreateMarketRequest) (CreateMarketResponse, error) {
	if !c.breaker.allow() {
		return CreateMarketResponse{}, apierrors.ErrEngineUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, engineCallTimeout)
	defer cancel()
	resp, err := c.client.CreateMarket(ctx, &enginev1.CreateMarketRequest{
		MarketId:         req.MarketID,
		Auction:          req.Auction,
		AuctionPreopenMs: req.AuctionPreOpenMs,
		ReferencePrice:   req.ReferencePrice,
	})
	c.breaker.record(err)
	if err != nil {
		return CreateMarketResponse{}, err
	}
	return CreateMarketResponse{Created: resp.Created, State: resp.State}, nil
}

func (c *EngineClient) GetStats(ctx context.Context, marketID string) (MarketStats, error) {
	if !c.breaker.allow() {
		return MarketStats{}, apierrors.ErrEngineUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, engineCallTimeout)
	defer cancel()
	resp, err := c.client.GetStats(ctx, &enginev1.GetStatsRequest{MarketId: marketID})
	c.breaker.record(err)
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
