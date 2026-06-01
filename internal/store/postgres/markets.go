package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// FeeTierRow is one volume-based fee tier as stored in the markets.fee_tiers JSONB.
// All values are decimal strings: MinVolume at the market's price precision,
// rates at precision 4 (e.g. "0.0010" = 0.10%).
type FeeTierRow struct {
	MinVolume    string `json:"minVolume"`
	MakerFeeRate string `json:"makerFeeRate"`
	TakerFeeRate string `json:"takerFeeRate"`
}

// MarketRow mirrors the markets table.
type MarketRow struct {
	MarketID       string
	BaseAsset      string
	QuoteAsset     string
	PricePrecision uint8
	QtyPrecision   uint8
	TickSize       int64
	LotSize        int64
	MinOrderQty    int64
	MaxOrderQty    int64
	MaxOrderValue  int64
	MaxDepth       int
	Features       int
	STPMode        string
	MakerFeeRate   int64
	TakerFeeRate   int64
	FeeCurrency    string
	FeeModel       string
	FeeTiers       []FeeTierRow
	State          string
	CreatedBy      string
}

const marketCols = `market_id, COALESCE(base_asset,''), COALESCE(quote_asset,''),
	price_precision, qty_precision, tick_size, lot_size,
	COALESCE(min_order_qty,0), COALESCE(max_order_qty,0), COALESCE(max_order_value,0),
	COALESCE(max_depth,0), features, COALESCE(stp_mode,''),
	maker_fee_rate, taker_fee_rate, COALESCE(fee_currency,''),
	fee_model, COALESCE(fee_tiers,'[]'::jsonb), state, COALESCE(created_by,'')`

// scanMarket scans a row produced by a SELECT of marketCols.
func scanMarket(row pgx.Row) (MarketRow, error) {
	var m MarketRow
	var tiersJSON []byte
	err := row.Scan(&m.MarketID, &m.BaseAsset, &m.QuoteAsset,
		&m.PricePrecision, &m.QtyPrecision, &m.TickSize, &m.LotSize,
		&m.MinOrderQty, &m.MaxOrderQty, &m.MaxOrderValue,
		&m.MaxDepth, &m.Features, &m.STPMode,
		&m.MakerFeeRate, &m.TakerFeeRate, &m.FeeCurrency,
		&m.FeeModel, &tiersJSON, &m.State, &m.CreatedBy)
	if err != nil {
		return MarketRow{}, err
	}
	if len(tiersJSON) > 0 {
		if err := json.Unmarshal(tiersJSON, &m.FeeTiers); err != nil {
			return MarketRow{}, errors.New("market " + m.MarketID + ": invalid fee_tiers JSON: " + err.Error())
		}
	}
	return m, nil
}

// GetMarket fetches a market row by ID.
func GetMarket(ctx context.Context, pool *pgxpool.Pool, marketID string) (MarketRow, error) {
	m, err := scanMarket(pool.QueryRow(ctx,
		`SELECT `+marketCols+` FROM markets WHERE market_id=$1`, marketID))
	if errors.Is(err, pgx.ErrNoRows) {
		return MarketRow{}, errors.New("market not found: " + marketID)
	}
	return m, err
}

// ListMarkets returns all markets.
func ListMarkets(ctx context.Context, pool *pgxpool.Pool) ([]MarketRow, error) {
	rows, err := pool.Query(ctx, `SELECT `+marketCols+` FROM markets ORDER BY market_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []MarketRow
	for rows.Next() {
		m, err := scanMarket(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}

// InsertMarket inserts a new market row.
func InsertMarket(ctx context.Context, pool *pgxpool.Pool, m MarketRow) error {
	tiersJSON, err := json.Marshal(m.FeeTiers)
	if err != nil {
		return err
	}
	if len(m.FeeTiers) == 0 {
		tiersJSON = []byte("[]")
	}
	_, err = pool.Exec(ctx,
		`INSERT INTO markets
		 (market_id, base_asset, quote_asset, price_precision, qty_precision,
		  tick_size, lot_size, min_order_qty, max_order_qty, max_order_value,
		  max_depth, features, stp_mode, maker_fee_rate, taker_fee_rate,
		  fee_currency, fee_model, fee_tiers, state, created_by)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)`,
		m.MarketID, m.BaseAsset, m.QuoteAsset, m.PricePrecision, m.QtyPrecision,
		m.TickSize, m.LotSize, m.MinOrderQty, m.MaxOrderQty, m.MaxOrderValue,
		m.MaxDepth, m.Features, m.STPMode, m.MakerFeeRate, m.TakerFeeRate,
		m.FeeCurrency, m.FeeModel, tiersJSON, m.State, m.CreatedBy,
	)
	return err
}

// UpdateMarketState updates the market's state field.
func UpdateMarketState(ctx context.Context, pool *pgxpool.Pool, marketID, state string) error {
	_, err := pool.Exec(ctx,
		`UPDATE markets SET state=$2, updated_at=now() WHERE market_id=$1`,
		marketID, state,
	)
	return err
}
