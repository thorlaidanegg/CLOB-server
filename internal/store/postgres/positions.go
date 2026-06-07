package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PositionRow mirrors the positions table.
type PositionRow struct {
	UserID         string
	MarketID       string
	Quantity       int64
	AvgEntryPrice  int64
	RealisedPnl    int64
	LastEventSeq   int64
}

// GrantPosition upserts a position's quantity and average entry price. Operational
// tool (admin) for seeding base inventory on a market — the only way to bootstrap
// the sell side on a fresh long-only market. Quantity/avgEntryPrice are raw
// integers at the market's qty/price precision.
func GrantPosition(ctx context.Context, pool *pgxpool.Pool, userID, marketID string, quantity, avgEntryPrice int64) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO positions (user_id, market_id, quantity, avg_entry_price, realised_pnl, last_event_seq)
		 VALUES ($1, $2, $3, $4, 0, 0)
		 ON CONFLICT (user_id, market_id)
		 DO UPDATE SET quantity = EXCLUDED.quantity, avg_entry_price = EXCLUDED.avg_entry_price, updated_at = now()`,
		userID, marketID, quantity, avgEntryPrice,
	)
	return err
}

// GetPosition fetches a single position. Returns a zero-value row (not an error) if not found.
func GetPosition(ctx context.Context, pool *pgxpool.Pool, userID, marketID string) (PositionRow, error) {
	var r PositionRow
	err := pool.QueryRow(ctx,
		`SELECT user_id, market_id, quantity, avg_entry_price, realised_pnl, last_event_seq
		 FROM positions WHERE user_id=$1 AND market_id=$2`,
		userID, marketID,
	).Scan(&r.UserID, &r.MarketID, &r.Quantity, &r.AvgEntryPrice, &r.RealisedPnl, &r.LastEventSeq)
	if errors.Is(err, pgx.ErrNoRows) {
		return PositionRow{UserID: userID, MarketID: marketID}, nil
	}
	return r, err
}

// ListPositionsByUser returns all positions for a user.
func ListPositionsByUser(ctx context.Context, pool *pgxpool.Pool, userID string) ([]PositionRow, error) {
	rows, err := pool.Query(ctx,
		`SELECT user_id, market_id, quantity, avg_entry_price, realised_pnl, last_event_seq
		 FROM positions WHERE user_id=$1`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []PositionRow
	for rows.Next() {
		var r PositionRow
		if err := rows.Scan(&r.UserID, &r.MarketID, &r.Quantity, &r.AvgEntryPrice, &r.RealisedPnl, &r.LastEventSeq); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ListAllPositions returns every position row. Used by leaderboard worker startup seed.
func ListAllPositions(ctx context.Context, pool *pgxpool.Pool) ([]PositionRow, error) {
	rows, err := pool.Query(ctx,
		`SELECT user_id, market_id, quantity, avg_entry_price, realised_pnl, last_event_seq FROM positions`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []PositionRow
	for rows.Next() {
		var r PositionRow
		if err := rows.Scan(&r.UserID, &r.MarketID, &r.Quantity, &r.AvgEntryPrice, &r.RealisedPnl, &r.LastEventSeq); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}
