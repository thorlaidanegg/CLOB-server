package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// WalletRow mirrors the wallets table.
type WalletRow struct {
	UserID    string
	Available int64
	Reserved  int64
	Precision uint8
}

// GetWallet fetches a wallet row.
func GetWallet(ctx context.Context, pool *pgxpool.Pool, userID string) (WalletRow, error) {
	var w WalletRow
	err := pool.QueryRow(ctx,
		`SELECT user_id, available, reserved, precision FROM wallets WHERE user_id=$1`,
		userID,
	).Scan(&w.UserID, &w.Available, &w.Reserved, &w.Precision)
	if err != nil {
		return WalletRow{}, fmt.Errorf("postgres: get wallet %s: %w", userID, err)
	}
	return w, nil
}

// UpsertWallet creates or resets a wallet row.
func UpsertWallet(ctx context.Context, pool *pgxpool.Pool, userID string, precision uint8) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO wallets (user_id, precision) VALUES ($1, $2)
		 ON CONFLICT (user_id) DO NOTHING`,
		userID, precision,
	)
	return err
}

// CreditWallet increases available balance.
func CreditWallet(ctx context.Context, pool *pgxpool.Pool, userID string, amount int64) error {
	tag, err := pool.Exec(ctx,
		`UPDATE wallets SET available = available + $2, version = version + 1, updated_at = now()
		 WHERE user_id = $1`,
		userID, amount,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("postgres: wallet not found: %s", userID)
	}
	return nil
}
