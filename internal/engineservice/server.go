package engineservice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog"
	enginev1 "github.com/thorlaidanegg/clob-server/proto/engine/v1"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EngineServer implements the gRPC EngineService.
type EngineServer struct {
	enginev1.UnimplementedEngineServiceServer
	multi  *engine.MultiEngine
	logger zerolog.Logger
}

// NewEngineServer creates a gRPC server wrapping a MultiEngine.
func NewEngineServer(multi *engine.MultiEngine, logger zerolog.Logger) *EngineServer {
	return &EngineServer{multi: multi, logger: logger}
}

func (s *EngineServer) PlaceOrder(ctx context.Context, req *enginev1.PlaceOrderRequest) (*enginev1.PlaceOrderResponse, error) {
	adapter := &directAdapterForGRPC{multi: s.multi}
	resp, err := adapter.placeFromProto(ctx, req)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &enginev1.PlaceOrderResponse{
		OrderId: resp.OrderID,
		Status:  resp.Status,
		Reason:  resp.Reason,
	}, nil
}

func (s *EngineServer) CancelOrder(ctx context.Context, req *enginev1.CancelOrderRequest) (*enginev1.CancelOrderResponse, error) {
	cmd := engine.CancelOrder{
		MarketID: types.MarketID(req.MarketId),
		OrderID:  types.OrderID(req.OrderId),
		UserID:   types.UserID(req.UserId),
	}
	if err := s.multi.Submit(cmd); err != nil {
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &enginev1.CancelOrderResponse{OrderId: req.OrderId, Status: "accepted"}, nil
}

func (s *EngineServer) GetDepth(_ context.Context, req *enginev1.GetDepthRequest) (*enginev1.GetDepthResponse, error) {
	snap, err := s.multi.Snapshot(types.MarketID(req.MarketId), int(req.Levels))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "market not found: %s", req.MarketId)
	}

	resp := &enginev1.GetDepthResponse{
		Bids: make([]*enginev1.DepthLevel, len(snap.Bids)),
		Asks: make([]*enginev1.DepthLevel, len(snap.Asks)),
	}
	for i, b := range snap.Bids {
		resp.Bids[i] = &enginev1.DepthLevel{
			Price: b.Price.String(), TotalQty: b.TotalQty.String(),
			DisplayQty: b.DisplayQty.String(), OrderCount: int32(b.OrderCount),
		}
	}
	for i, a := range snap.Asks {
		resp.Asks[i] = &enginev1.DepthLevel{
			Price: a.Price.String(), TotalQty: a.TotalQty.String(),
			DisplayQty: a.DisplayQty.String(), OrderCount: int32(a.OrderCount),
		}
	}
	return resp, nil
}

func (s *EngineServer) GetBBO(_ context.Context, req *enginev1.GetBBORequest) (*enginev1.GetBBOResponse, error) {
	bid, ask, hasBid, hasAsk, err := s.multi.BBO(types.MarketID(req.MarketId))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "market not found: %s", req.MarketId)
	}
	resp := &enginev1.GetBBOResponse{}
	if hasBid {
		resp.Bid = bid.String()
	}
	if hasAsk {
		resp.Ask = ask.String()
	}
	return resp, nil
}

func (s *EngineServer) StreamEvents(req *enginev1.StreamEventsRequest, stream enginev1.EngineService_StreamEventsServer) error {
	evCh, err := s.multi.Events(types.MarketID(req.MarketId))
	if err != nil {
		return status.Errorf(codes.NotFound, "market not found: %s", req.MarketId)
	}
	for {
		select {
		case ev, ok := <-evCh:
			if !ok {
				return nil
			}
			payload, _ := json.Marshal(ev)
			if err := stream.Send(&enginev1.EngineEvent{
				EventType: ev.Type(),
				Payload:   payload,
				SeqNum:    ev.SeqNum(),
			}); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return nil
		}
	}
}

// directAdapterForGRPC is a thin helper for converting proto requests to engine commands.
type directAdapterForGRPC struct {
	multi *engine.MultiEngine
}

func (a *directAdapterForGRPC) placeFromProto(ctx context.Context, req *enginev1.PlaceOrderRequest) (struct{ OrderID, Status, Reason string }, error) {
	// Re-use the existing directAdapter parsing logic.
	from := newGRPCBridge(a.multi)
	return from.place(ctx, req)
}

func parseTIFStr(s string) (types.TIF, error) {
	switch s {
	case "GTC", "":
		return types.GTC, nil
	case "IOC":
		return types.IOC, nil
	case "FOK":
		return types.FOK, nil
	case "GTD":
		return types.GTD, nil
	case "DAY":
		return types.DAY, nil
	default:
		return 0, fmt.Errorf("unknown TIF: %s", s)
	}
}

// newGRPCBridge creates the bridge. Defined here to avoid circular imports.
func newGRPCBridge(multi *engine.MultiEngine) *grpcBridge { return &grpcBridge{multi: multi} }

type grpcBridge struct{ multi *engine.MultiEngine }

func (b *grpcBridge) place(_ context.Context, req *enginev1.PlaceOrderRequest) (struct{ OrderID, Status, Reason string }, error) {
	side, err := types.SideFromString(req.Side)
	if err != nil {
		return struct{ OrderID, Status, Reason string }{}, err
	}
	tif, err := parseTIFStr(req.Tif)
	if err != nil {
		return struct{ OrderID, Status, Reason string }{}, err
	}

	pp := uint8(2)
	qp := uint8(2)

	switch req.OrderType {
	case "limit", "iceberg", "":
		price, _ := types.ParseDecimal(req.Price, pp)
		qty, _ := types.ParseDecimal(req.Qty, qp)
		b.multi.Submit(engine.PlaceLimitOrder{
			MarketID: types.MarketID(req.MarketId),
			OrderID:  types.OrderID(req.OrderId),
			UserID:   types.UserID(req.UserId),
			Side: side, Price: price, Qty: qty, TIF: tif, ExpireAt: req.ExpireAt,
		})
	case "market":
		qty, _ := types.ParseDecimal(req.Qty, qp)
		b.multi.Submit(engine.PlaceMarketOrder{
			MarketID: types.MarketID(req.MarketId), OrderID: types.OrderID(req.OrderId),
			UserID: types.UserID(req.UserId), Side: side, Qty: qty, TIF: tif,
		})
	}
	return struct{ OrderID, Status, Reason string }{req.OrderId, "accepted", ""}, nil
}
