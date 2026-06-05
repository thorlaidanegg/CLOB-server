package engineservice

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/rs/zerolog"
	clobconfig "github.com/thorlaidanegg/clob/config"
	enginev1 "github.com/thorlaidanegg/clob-server/proto/engine/v1"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// EngineServer implements the gRPC EngineService.
type EngineServer struct {
	enginev1.UnimplementedEngineServiceServer
	multi       *engine.MultiEngine
	marketCfgs  map[string]clobconfig.MarketConfig // keyed by marketID string
	logger      zerolog.Logger
}

// NewEngineServer creates a gRPC EngineService server.
// marketCfgs must include all markets the engine was created with so that price/qty
// precision is known for correct Decimal parsing.
func NewEngineServer(multi *engine.MultiEngine, cfgs []clobconfig.MarketConfig, logger zerolog.Logger) *EngineServer {
	m := make(map[string]clobconfig.MarketConfig, len(cfgs))
	for _, c := range cfgs {
		m[string(c.MarketID)] = c
	}
	return &EngineServer{multi: multi, marketCfgs: m, logger: logger}
}

func (s *EngineServer) PlaceOrder(ctx context.Context, req *enginev1.PlaceOrderRequest) (*enginev1.PlaceOrderResponse, error) {
	mc, ok := s.marketCfgs[req.MarketId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "market not found: %s", req.MarketId)
	}

	orderID, st, reason, err := s.placeOrder(req, mc)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &enginev1.PlaceOrderResponse{
		OrderId: orderID,
		Status:  st,
		Reason:  reason,
	}, nil
}

func (s *EngineServer) placeOrder(req *enginev1.PlaceOrderRequest, mc clobconfig.MarketConfig) (orderID, st, reason string, err error) {
	side, err := types.SideFromString(req.Side)
	if err != nil {
		return "", "", "", err
	}
	tif, err := parseTIFStr(req.Tif)
	if err != nil {
		return "", "", "", err
	}

	pp := mc.PricePrecision
	qp := mc.QtyPrecision

	switch req.OrderType {
	case "limit", "iceberg", "":
		price, err := types.ParseDecimal(req.Price, pp)
		if err != nil {
			return "", "", "", fmt.Errorf("invalid price %q: %w", req.Price, err)
		}
		qty, err := types.ParseDecimal(req.Qty, qp)
		if err != nil {
			return "", "", "", fmt.Errorf("invalid qty %q: %w", req.Qty, err)
		}
		var displayQty types.Decimal
		if req.DisplayQty != "" {
			displayQty, err = types.ParseDecimal(req.DisplayQty, qp)
			if err != nil {
				return "", "", "", fmt.Errorf("invalid display_qty %q: %w", req.DisplayQty, err)
			}
		}
		if err := s.multi.Submit(engine.PlaceLimitOrder{
			MarketID: types.MarketID(req.MarketId), OrderID: types.OrderID(req.OrderId),
			UserID: types.UserID(req.UserId), Side: side, Price: price, Qty: qty,
			DisplayQty: displayQty, TIF: tif, ExpireAt: req.ExpireAt,
			Flags: parseOrderFlags(req.Flags), STPMode: parseSTPMode(req.StpMode),
		}); err != nil {
			return "", "", "", err
		}

	case "market":
		qty, err := types.ParseDecimal(req.Qty, qp)
		if err != nil {
			return "", "", "", fmt.Errorf("invalid qty %q: %w", req.Qty, err)
		}
		if err := s.multi.Submit(engine.PlaceMarketOrder{
			MarketID: types.MarketID(req.MarketId), OrderID: types.OrderID(req.OrderId),
			UserID: types.UserID(req.UserId), Side: side, Qty: qty, TIF: tif,
			Flags: parseOrderFlags(req.Flags), STPMode: parseSTPMode(req.StpMode),
		}); err != nil {
			return "", "", "", err
		}

	case "stop", "stop_limit":
		stopPrice, err := types.ParseDecimal(req.StopPrice, pp)
		if err != nil {
			return "", "", "", fmt.Errorf("invalid stop_price %q: %w", req.StopPrice, err)
		}
		qty, err := types.ParseDecimal(req.Qty, qp)
		if err != nil {
			return "", "", "", fmt.Errorf("invalid qty %q: %w", req.Qty, err)
		}
		var limitPrice types.Decimal
		convertTo := types.Market
		if req.OrderType == "stop_limit" {
			limitPrice, err = types.ParseDecimal(req.Price, pp)
			if err != nil {
				return "", "", "", fmt.Errorf("invalid price %q: %w", req.Price, err)
			}
			convertTo = types.Limit
		}
		if err := s.multi.Submit(engine.PlaceStopOrder{
			MarketID: types.MarketID(req.MarketId), OrderID: types.OrderID(req.OrderId),
			UserID: types.UserID(req.UserId), Side: side, TriggerPrice: stopPrice,
			LimitPrice: limitPrice, Qty: qty, ConvertTo: convertTo,
			TIF: tif, ExpireAt: req.ExpireAt,
			Flags: parseOrderFlags(req.Flags), STPMode: parseSTPMode(req.StpMode),
		}); err != nil {
			return "", "", "", err
		}

	default:
		return "", "", "", fmt.Errorf("unknown order_type: %s", req.OrderType)
	}

	return req.OrderId, "accepted", "", nil
}

// parseOrderFlags maps the wire flag strings to the engine's OrderFlags bitfield.
// Unknown flags are ignored. (Iceberg is driven by DisplayQty, not a flag here.)
func parseOrderFlags(flags []string) types.OrderFlags {
	var f types.OrderFlags
	for _, s := range flags {
		switch s {
		case "post_only":
			f = f.Set(types.FlagPostOnly)
		case "reduce_only":
			f = f.Set(types.FlagReduceOnly)
		}
	}
	return f
}

// parseSTPMode maps a per-order self-trade-prevention mode string to the engine
// enum. Empty/unknown means "use the market default" (STPDisabled here).
func parseSTPMode(s string) clobconfig.STPMode {
	switch s {
	case "cancel_both":
		return clobconfig.STPCancelBoth
	case "cancel_maker":
		return clobconfig.STPCancelMaker
	case "cancel_taker":
		return clobconfig.STPCancelTaker
	case "decrement_cancel":
		return clobconfig.STPDecrementCancel
	default:
		return clobconfig.STPDisabled
	}
}

func (s *EngineServer) CancelOrder(_ context.Context, req *enginev1.CancelOrderRequest) (*enginev1.CancelOrderResponse, error) {
	if err := s.multi.Submit(engine.CancelOrder{
		MarketID: types.MarketID(req.MarketId),
		OrderID:  types.OrderID(req.OrderId),
		UserID:   types.UserID(req.UserId),
	}); err != nil {
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

func (s *EngineServer) GetStats(_ context.Context, req *enginev1.GetStatsRequest) (*enginev1.GetStatsResponse, error) {
	st, err := s.multi.Stats(types.MarketID(req.MarketId))
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "market not found: %s", req.MarketId)
	}
	return &enginev1.GetStatsResponse{
		MarketId:          string(st.MarketID),
		State:             st.State.String(),
		OrderSeq:          st.OrderSeq,
		EventSeq:          st.EventSeq,
		OpenOrders:        int32(st.OpenOrders),
		StopOrders:        int32(st.StopOrders),
		BidLevels:         int32(st.BidLevels),
		AskLevels:         int32(st.AskLevels),
		NodePoolUsed:      int32(st.NodePoolUsed),
		NodePoolCapacity:  int32(st.NodePoolCapacity),
		LevelPoolUsed:     int32(st.LevelPoolUsed),
		LevelPoolCapacity: int32(st.LevelPoolCapacity),
	}, nil
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
