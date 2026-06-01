package client

import (
	"context"
	"fmt"

	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
)

// directAdapter implements EngineAdapter by calling MultiEngine directly (ROLE=all).
type directAdapter struct {
	multi       *engine.MultiEngine
	marketCfgs  map[string]clobconfig.MarketConfig
	orderStore  ordersstore.Store
}

// NewDirectAdapter creates an in-process adapter.
// orderStore is used by CancelOrder to resolve marketID from orderID.
func NewDirectAdapter(multi *engine.MultiEngine, cfgs []clobconfig.MarketConfig, orderStore ordersstore.Store) EngineAdapter {
	m := make(map[string]clobconfig.MarketConfig, len(cfgs))
	for _, c := range cfgs {
		m[string(c.MarketID)] = c
	}
	return &directAdapter{multi: multi, marketCfgs: m, orderStore: orderStore}
}

func (a *directAdapter) PlaceOrder(ctx context.Context, req PlaceOrderRequest) (PlaceOrderResponse, error) {
	mc, ok := a.marketCfgs[req.MarketID]
	if !ok {
		return PlaceOrderResponse{}, fmt.Errorf("market not found: %s", req.MarketID)
	}

	side, err := types.SideFromString(req.Side)
	if err != nil {
		return PlaceOrderResponse{}, err
	}

	tif, err := parseTIF(req.TIF)
	if err != nil {
		return PlaceOrderResponse{}, err
	}

	switch req.OrderType {
	case "limit", "iceberg":
		price, err := types.ParseDecimal(req.Price, mc.PricePrecision)
		if err != nil {
			return PlaceOrderResponse{}, fmt.Errorf("invalid price: %w", err)
		}
		qty, err := types.ParseDecimal(req.Qty, mc.QtyPrecision)
		if err != nil {
			return PlaceOrderResponse{}, fmt.Errorf("invalid qty: %w", err)
		}
		var displayQty types.Decimal
		if req.DisplayQty != "" {
			displayQty, err = types.ParseDecimal(req.DisplayQty, mc.QtyPrecision)
			if err != nil {
				return PlaceOrderResponse{}, fmt.Errorf("invalid display_qty: %w", err)
			}
		}
		cmd := engine.PlaceLimitOrder{
			MarketID:   types.MarketID(req.MarketID),
			OrderID:    types.OrderID(req.OrderID),
			UserID:     types.UserID(req.UserID),
			Side:       side,
			Price:      price,
			Qty:        qty,
			DisplayQty: displayQty,
			TIF:        tif,
			ExpireAt:   req.ExpireAt,
		}
		if err := a.multi.Submit(cmd); err != nil {
			return PlaceOrderResponse{}, err
		}

	case "market":
		qty, err := types.ParseDecimal(req.Qty, mc.QtyPrecision)
		if err != nil {
			return PlaceOrderResponse{}, fmt.Errorf("invalid qty: %w", err)
		}
		cmd := engine.PlaceMarketOrder{
			MarketID: types.MarketID(req.MarketID),
			OrderID:  types.OrderID(req.OrderID),
			UserID:   types.UserID(req.UserID),
			Side:     side,
			Qty:      qty,
			TIF:      tif,
		}
		if err := a.multi.Submit(cmd); err != nil {
			return PlaceOrderResponse{}, err
		}

	case "stop", "stop_limit":
		stopPrice, err := types.ParseDecimal(req.StopPrice, mc.PricePrecision)
		if err != nil {
			return PlaceOrderResponse{}, fmt.Errorf("invalid stop_price: %w", err)
		}
		qty, err := types.ParseDecimal(req.Qty, mc.QtyPrecision)
		if err != nil {
			return PlaceOrderResponse{}, fmt.Errorf("invalid qty: %w", err)
		}
		var limitPrice types.Decimal
		convertTo := types.Market
		if req.OrderType == "stop_limit" {
			limitPrice, err = types.ParseDecimal(req.Price, mc.PricePrecision)
			if err != nil {
				return PlaceOrderResponse{}, fmt.Errorf("invalid price: %w", err)
			}
			convertTo = types.Limit
		}
		cmd := engine.PlaceStopOrder{
			MarketID:     types.MarketID(req.MarketID),
			OrderID:      types.OrderID(req.OrderID),
			UserID:       types.UserID(req.UserID),
			Side:         side,
			TriggerPrice: stopPrice,
			LimitPrice:   limitPrice,
			Qty:          qty,
			ConvertTo:    convertTo,
			TIF:          tif,
			ExpireAt:     req.ExpireAt,
		}
		if err := a.multi.Submit(cmd); err != nil {
			return PlaceOrderResponse{}, err
		}

	default:
		return PlaceOrderResponse{}, fmt.Errorf("unknown order_type: %s", req.OrderType)
	}

	return PlaceOrderResponse{OrderID: req.OrderID, Status: "accepted"}, nil
}

func (a *directAdapter) CancelOrder(ctx context.Context, orderID, userID string) error {
	order, err := a.orderStore.GetOrder(ctx, orderID)
	if err != nil {
		return fmt.Errorf("cancel: order not found: %s", orderID)
	}
	return a.multi.Submit(engine.CancelOrder{
		MarketID: types.MarketID(order.MarketID),
		OrderID:  types.OrderID(orderID),
		UserID:   types.UserID(userID),
	})
}

func (a *directAdapter) GetDepth(ctx context.Context, marketID string, levels int) (events.BookSnapshot, error) {
	snap, err := a.multi.Snapshot(types.MarketID(marketID), levels)
	return snap, err
}

func (a *directAdapter) GetStats(_ context.Context, marketID string) (MarketStats, error) {
	st, err := a.multi.Stats(types.MarketID(marketID))
	if err != nil {
		return MarketStats{}, err
	}
	return MarketStats{
		MarketID:          string(st.MarketID),
		State:             st.State.String(),
		OrderSeq:          st.OrderSeq,
		EventSeq:          st.EventSeq,
		OpenOrders:        st.OpenOrders,
		StopOrders:        st.StopOrders,
		BidLevels:         st.BidLevels,
		AskLevels:         st.AskLevels,
		NodePoolUsed:      st.NodePoolUsed,
		NodePoolCapacity:  st.NodePoolCapacity,
		LevelPoolUsed:     st.LevelPoolUsed,
		LevelPoolCapacity: st.LevelPoolCapacity,
	}, nil
}

func (a *directAdapter) GetBBO(_ context.Context, marketID string) (bid, ask string, err error) {
	bidD, askD, hasBid, hasAsk, err := a.multi.BBO(types.MarketID(marketID))
	if err != nil {
		return "", "", err
	}
	if hasBid {
		bid = bidD.String()
	}
	if hasAsk {
		ask = askD.String()
	}
	return bid, ask, nil
}

func parseTIF(s string) (types.TIF, error) {
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
