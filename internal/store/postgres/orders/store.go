package orders

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// OrderRow mirrors the orders table.
type OrderRow struct {
	OrderID         string
	UserID          string
	MarketID        string
	Side            string
	OrderType       string
	Price           int64
	StopPrice       int64
	OrigQty         int64
	RemainQty       int64
	FilledQty       int64
	DisplayQty      int64
	Status          string
	TIF             string
	Flags           int
	ReservedPerUnit int64
}

// Store defines order persistence operations.
type Store interface {
	InsertOrder(ctx context.Context, order OrderRow) error
	GetOrder(ctx context.Context, orderID string) (OrderRow, error)
	UpdateOrderStatus(ctx context.Context, orderID, status string) error
	UpdateReservedPerUnit(ctx context.Context, orderID string, val int64) error
	ListOpenOrders(ctx context.Context, userID string) ([]OrderRow, error)
}

// PgStore implements Store against Postgres.
type PgStore struct {
	pool *pgxpool.Pool
}

// NewPgStore constructs a PgStore.
func NewPgStore(pool *pgxpool.Pool) *PgStore {
	return &PgStore{pool: pool}
}

func (s *PgStore) InsertOrder(ctx context.Context, o OrderRow) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO orders
		 (order_id, user_id, market_id, side, order_type, price, stop_price,
		  orig_qty, remain_qty, filled_qty, display_qty, status, tif, flags)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		o.OrderID, o.UserID, o.MarketID, o.Side, o.OrderType, o.Price, o.StopPrice,
		o.OrigQty, o.RemainQty, o.FilledQty, o.DisplayQty, o.Status, o.TIF, o.Flags,
	)
	return err
}

func (s *PgStore) GetOrder(ctx context.Context, orderID string) (OrderRow, error) {
	var o OrderRow
	err := s.pool.QueryRow(ctx,
		`SELECT order_id, user_id, market_id, side, order_type, price, stop_price,
		        orig_qty, remain_qty, filled_qty, display_qty, status, tif, flags, reserved_per_unit
		 FROM orders WHERE order_id=$1`,
		orderID,
	).Scan(&o.OrderID, &o.UserID, &o.MarketID, &o.Side, &o.OrderType, &o.Price, &o.StopPrice,
		&o.OrigQty, &o.RemainQty, &o.FilledQty, &o.DisplayQty, &o.Status, &o.TIF, &o.Flags, &o.ReservedPerUnit)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrderRow{}, fmt.Errorf("orders: not found: %s", orderID)
	}
	return o, err
}

func (s *PgStore) UpdateOrderStatus(ctx context.Context, orderID, status string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE orders SET status=$2, updated_at=now() WHERE order_id=$1`,
		orderID, status,
	)
	return err
}

func (s *PgStore) UpdateReservedPerUnit(ctx context.Context, orderID string, val int64) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE orders SET reserved_per_unit=$2, updated_at=now() WHERE order_id=$1`,
		orderID, val,
	)
	return err
}

func (s *PgStore) ListOpenOrders(ctx context.Context, userID string) ([]OrderRow, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT order_id, user_id, market_id, side, order_type, price, stop_price,
		        orig_qty, remain_qty, filled_qty, display_qty, status, tif, flags, reserved_per_unit
		 FROM orders WHERE user_id=$1 AND status IN ('new','rested','partial')
		 ORDER BY created_at DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []OrderRow
	for rows.Next() {
		var o OrderRow
		if err := rows.Scan(&o.OrderID, &o.UserID, &o.MarketID, &o.Side, &o.OrderType, &o.Price, &o.StopPrice,
			&o.OrigQty, &o.RemainQty, &o.FilledQty, &o.DisplayQty, &o.Status, &o.TIF, &o.Flags, &o.ReservedPerUnit); err != nil {
			return nil, err
		}
		result = append(result, o)
	}
	return result, rows.Err()
}
