package settlement

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
)

// Handler processes settlement events.
type Handler struct {
	pool       *pgxpool.Pool
	orderStore ordersstore.Store
	logger     zerolog.Logger
}

// New creates a settlement Handler.
func New(pool *pgxpool.Pool, orderStore ordersstore.Store, logger zerolog.Logger) *Handler {
	return &Handler{pool: pool, orderStore: orderStore, logger: logger}
}

// HandleEvent dispatches to the correct handler by event type.
func (h *Handler) HandleEvent(ctx context.Context, tx pgx.Tx, env workers.EventEnvelope) error {
	switch env.EventType {
	case events.TypeTradeFill:
		fill, ok := env.Event.(*events.TradeFill)
		if !ok {
			return nil
		}
		return h.handleTradeFill(ctx, tx, fill)
	case events.TypeTradeExecuted:
		ev, ok := env.Event.(*events.TradeExecuted)
		if !ok {
			return nil
		}
		return h.handleTradeExecuted(ctx, tx, ev)
	case events.TypeOrderCanceled:
		ev, ok := env.Event.(*events.OrderCanceled)
		if !ok {
			return nil
		}
		return h.closeOrder(ctx, tx, string(ev.OrderID), string(ev.MarketID()), "canceled")
	case events.TypeOrderExpired:
		ev, ok := env.Event.(*events.OrderExpired)
		if !ok {
			return nil
		}
		// A GTD/DAY order that timed out: release its reservation exactly like a
		// cancel (otherwise the buyer's reserved credits leak).
		return h.closeOrder(ctx, tx, string(ev.OrderID), string(ev.MarketID()), "expired")
	case events.TypeOrderRejected:
		ev, ok := env.Event.(*events.OrderRejected)
		if !ok {
			return nil
		}
		return h.handleOrderRejected(ctx, tx, ev)
	}
	return nil
}

func (h *Handler) handleTradeExecuted(ctx context.Context, tx pgx.Tx, ev *events.TradeExecuted) error {
	return pgstore.InsertTradeTx(ctx, tx, pgstore.TradeRow{
		TradeID:      string(ev.TradeID),
		MarketID:     string(ev.MarketID()),
		MakerOrderID: string(ev.MakerOrderID),
		TakerOrderID: string(ev.TakerOrderID),
		MakerUserID:  string(ev.MakerUserID),
		TakerUserID:  string(ev.TakerUserID),
		MakerSide:    ev.MakerSide.String(),
		Price:        ev.Price.Value(),
		Qty:          ev.Qty.Value(),
		MakerFee:     ev.MakerFee.Value(),
		TakerFee:     ev.TakerFee.Value(),
		FeeCurrency:  ev.FeeCurrency,
		SeqNum:       int64(ev.SeqNum()),
		ExecutedAtNs: ev.Timestamp(),
	})
}

// handleTradeFill records fill progress on the order and settles credits.
// Credit movement branches by side, never by maker/taker role
// (see doc/WALLET_MODEL.md): a buyer consumes their reservation and pays the
// actual cost; a seller receives the sale proceeds.
func (h *Handler) handleTradeFill(ctx context.Context, tx pgx.Tx, fill *events.TradeFill) error {
	if err := h.updateOrderFill(ctx, tx, fill); err != nil {
		return err
	}
	if fill.Side == types.Bid {
		return h.settleBuyer(ctx, tx, fill)
	}
	return h.settleSeller(ctx, tx, fill)
}

// updateOrderFill advances the order's fill progress and status. The event's
// RemainQty is absolute, so filled_qty/remain_qty/status are set (not
// incremented) — safe to re-apply if the event is ever reprocessed.
func (h *Handler) updateOrderFill(ctx context.Context, tx pgx.Tx, fill *events.TradeFill) error {
	_, err := tx.Exec(ctx,
		`UPDATE orders SET
		   remain_qty = $2,
		   filled_qty = orig_qty - $2,
		   status     = CASE WHEN $2 = 0 THEN 'filled' ELSE 'partial' END,
		   updated_at = now()
		 WHERE order_id = $1`,
		string(fill.OrderID), fill.RemainQty.Value(),
	)
	if err != nil {
		return fmt.Errorf("settlement: update order fill: %w", err)
	}
	return nil
}

// settleBuyer releases the exact hook reservation and deducts the real cost
// atomically. Identical for maker and taker buyers — the only difference, the
// fee, is already in fill.Fee.
func (h *Handler) settleBuyer(ctx context.Context, tx pgx.Tx, fill *events.TradeFill) error {
	userID := string(fill.UserID)

	order, err := h.orderStore.GetOrder(ctx, string(fill.OrderID))
	if err != nil {
		return fmt.Errorf("settlement: get buyer order: %w", err)
	}
	market, err := pgstore.GetMarket(ctx, h.pool, string(fill.MarketID()))
	if err != nil {
		return fmt.Errorf("settlement: get market: %w", err)
	}

	reservedPerUnit := types.NewDecimal(order.ReservedPerUnit, market.PricePrecision)
	reservationForFill := reservedPerUnit.MulQty(fill.FilledQty)
	cost := fill.Price.MulQty(fill.FilledQty).Add(fill.Fee)

	tag, err := tx.Exec(ctx,
		`UPDATE wallets SET
		   reserved  = reserved  - $2,
		   available = available + $2 - $3,
		   version   = version   + 1,
		   updated_at = now()
		 WHERE user_id=$1 AND reserved >= $2`,
		userID, reservationForFill.Value(), cost.Value(),
	)
	if err != nil {
		return fmt.Errorf("settlement: buyer wallet update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		h.logger.Error().Str("userID", userID).Msg("settlement: buyer wallet anomaly — insufficient reserved")
	}
	return nil
}

// settleSeller credits the sale proceeds. Sellers reserved no credits (long-only),
// so reserved is untouched.
func (h *Handler) settleSeller(ctx context.Context, tx pgx.Tx, fill *events.TradeFill) error {
	userID := string(fill.UserID)
	proceeds := fill.Price.MulQty(fill.FilledQty).Sub(fill.Fee)

	tag, err := tx.Exec(ctx,
		`UPDATE wallets SET
		   available  = available + $2,
		   version    = version   + 1,
		   updated_at = now()
		 WHERE user_id=$1`,
		userID, proceeds.Value(),
	)
	if err != nil {
		return fmt.Errorf("settlement: seller wallet update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		h.logger.Error().Str("userID", userID).Msg("settlement: seller wallet anomaly — wallet missing")
	}
	return nil
}

// closeOrder releases an open order's outstanding reservation and marks it with
// the terminal status. Shared by cancel and expiry — both free the remaining
// reserved credits (buys only; sells reserved nothing) and close the order.
//
// The release is idempotent: it only fires when the order actually transitions
// from an open state to terminal. A second terminal event for the same order
// (e.g. repeated cancels producing "order not found" rejects) is a no-op, so the
// reservation is never released twice (which would drive reserved negative and
// trip the reserved_non_negative constraint).
func (h *Handler) closeOrder(ctx context.Context, tx pgx.Tx, orderID, marketID, status string) error {
	order, err := h.orderStore.GetOrder(ctx, orderID)
	if err != nil {
		return fmt.Errorf("settlement: get %s order: %w", status, err)
	}

	tag, err := tx.Exec(ctx,
		`UPDATE orders SET status=$2, updated_at=now()
		 WHERE order_id=$1 AND status IN ('new','rested','partial')`,
		order.OrderID, status,
	)
	if err != nil {
		return fmt.Errorf("settlement: %s order status: %w", status, err)
	}
	if tag.RowsAffected() == 0 {
		return nil // already terminal — reservation already released
	}

	// Only buys reserved credits (reserved_per_unit > 0). Sells reserved nothing,
	// so there is nothing to release — never fall back to price × qty for a sell.
	if order.ReservedPerUnit > 0 {
		market, err := pgstore.GetMarket(ctx, h.pool, marketID)
		if err != nil {
			return fmt.Errorf("settlement: get market for %s: %w", status, err)
		}
		qty := types.NewDecimal(order.RemainQty, market.QtyPrecision)
		reservedPerUnit := types.NewDecimal(order.ReservedPerUnit, market.PricePrecision)
		release := reservedPerUnit.MulQty(qty)

		if _, err := tx.Exec(ctx,
			`UPDATE wallets SET
			   reserved  = reserved  - $2,
			   available = available + $2,
			   version   = version   + 1,
			   updated_at = now()
			 WHERE user_id=$1`,
			order.UserID, release.Value(),
		); err != nil {
			return fmt.Errorf("settlement: %s wallet release: %w", status, err)
		}
	}
	return nil
}

func (h *Handler) handleOrderRejected(ctx context.Context, tx pgx.Tx, ev *events.OrderRejected) error {
	order, err := h.orderStore.GetOrder(ctx, string(ev.OrderID))
	if err != nil {
		// Order might not exist (e.g., engine reject before gateway insert) — non-fatal.
		return nil
	}

	// Idempotent (see closeOrder): only release if this actually closes an open
	// order. A reject that follows a cancel ("order not found") is a no-op.
	tag, err := tx.Exec(ctx,
		`UPDATE orders SET status='rejected', updated_at=now()
		 WHERE order_id=$1 AND status IN ('new','rested','partial')`,
		string(ev.OrderID),
	)
	if err != nil {
		return fmt.Errorf("settlement: reject order status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil
	}

	if order.ReservedPerUnit > 0 {
		market, err := pgstore.GetMarket(ctx, h.pool, order.MarketID)
		if err != nil {
			return fmt.Errorf("settlement: get market for reject: %w", err)
		}
		reservedPerUnit := types.NewDecimal(order.ReservedPerUnit, market.PricePrecision)
		qty := types.NewDecimal(order.OrigQty, market.QtyPrecision)
		release := reservedPerUnit.MulQty(qty)

		if _, err := tx.Exec(ctx,
			`UPDATE wallets SET
			   reserved  = reserved  - $2,
			   available = available + $2,
			   version   = version   + 1,
			   updated_at = now()
			 WHERE user_id=$1`,
			order.UserID, release.Value(),
		); err != nil {
			return fmt.Errorf("settlement: reject wallet release: %w", err)
		}
	}
	return nil
}
