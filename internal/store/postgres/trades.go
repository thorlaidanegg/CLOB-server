package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TradeRow mirrors the trades table.
type TradeRow struct {
	TradeID       string
	MarketID      string
	MakerOrderID  string
	TakerOrderID  string
	MakerUserID   string
	TakerUserID   string
	MakerSide     string
	Price         int64
	Qty           int64
	MakerFee      int64
	TakerFee      int64
	FeeCurrency   string
	SeqNum        int64
	CreatedAt     string
}

// InsertTrade writes a single trade record. Ignores duplicate trade_id (idempotent).
func InsertTrade(ctx context.Context, pool *pgxpool.Pool, t TradeRow) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO trades
		 (trade_id, market_id, maker_order_id, taker_order_id, maker_user_id, taker_user_id,
		  maker_side, price, qty, maker_fee, taker_fee, fee_currency, seq_num)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		 ON CONFLICT (trade_id, market_id) DO NOTHING`,
		t.TradeID, t.MarketID, t.MakerOrderID, t.TakerOrderID, t.MakerUserID, t.TakerUserID,
		t.MakerSide, t.Price, t.Qty, t.MakerFee, t.TakerFee, t.FeeCurrency, t.SeqNum,
	)
	return err
}

// InsertTradeTx writes a trade record inside an existing transaction.
func InsertTradeTx(ctx context.Context, tx pgx.Tx, t TradeRow) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO trades
		 (trade_id, market_id, maker_order_id, taker_order_id, maker_user_id, taker_user_id,
		  maker_side, price, qty, maker_fee, taker_fee, fee_currency, seq_num)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		 ON CONFLICT (trade_id, market_id) DO NOTHING`,
		t.TradeID, t.MarketID, t.MakerOrderID, t.TakerOrderID, t.MakerUserID, t.TakerUserID,
		t.MakerSide, t.Price, t.Qty, t.MakerFee, t.TakerFee, t.FeeCurrency, t.SeqNum,
	)
	return err
}

// ListTradesByMarket returns the most recent trades for a market (newest first).
func ListTradesByMarket(ctx context.Context, pool *pgxpool.Pool, marketID string, limit int) ([]TradeRow, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := pool.Query(ctx,
		`SELECT trade_id, market_id, maker_order_id, taker_order_id, maker_user_id, taker_user_id,
		        maker_side, price, qty, maker_fee, taker_fee, COALESCE(fee_currency,''), seq_num,
		        to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
		 FROM trades WHERE market_id=$1
		 ORDER BY created_at DESC
		 LIMIT $2`,
		marketID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []TradeRow
	for rows.Next() {
		var t TradeRow
		if err := rows.Scan(
			&t.TradeID, &t.MarketID, &t.MakerOrderID, &t.TakerOrderID,
			&t.MakerUserID, &t.TakerUserID, &t.MakerSide,
			&t.Price, &t.Qty, &t.MakerFee, &t.TakerFee, &t.FeeCurrency, &t.SeqNum,
			&t.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, rows.Err()
}
