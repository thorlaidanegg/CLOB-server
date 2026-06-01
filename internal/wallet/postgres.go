package wallet

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/thorlaidanegg/clob/types"
)

// PgStore implements wallet.Store against Postgres.
type PgStore struct {
	pool      *pgxpool.Pool
	precision uint8
}

// NewPgStore creates a new wallet store. precision is the wallet currency precision.
func NewPgStore(pool *pgxpool.Pool, precision uint8) *PgStore {
	return &PgStore{pool: pool, precision: precision}
}

func (s *PgStore) GetAvailable(ctx context.Context, userID string) (types.Decimal, error) {
	var available int64
	err := s.pool.QueryRow(ctx,
		`SELECT available FROM wallets WHERE user_id=$1`, userID,
	).Scan(&available)
	if err != nil {
		return types.Decimal{}, fmt.Errorf("wallet: get available for %s: %w", userID, err)
	}
	return types.NewDecimal(available, s.precision), nil
}

// Reserve moves amount from available to reserved. Returns ErrInsufficientCredits if insufficient.
func (s *PgStore) Reserve(ctx context.Context, userID string, amount types.Decimal) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE wallets SET
		   available  = available  - $2,
		   reserved   = reserved   + $2,
		   version    = version    + 1,
		   updated_at = now()
		 WHERE user_id=$1 AND available >= $2`,
		userID, amount.Value(),
	)
	if err != nil {
		return fmt.Errorf("wallet: reserve: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrInsufficientCredits
	}
	return nil
}

// Release moves amount from reserved back to available.
func (s *PgStore) Release(ctx context.Context, userID string, amount types.Decimal) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE wallets SET
		   reserved   = reserved   - $2,
		   available  = available  + $2,
		   version    = version    + 1,
		   updated_at = now()
		 WHERE user_id=$1`,
		userID, amount.Value(),
	)
	return err
}

// Credit increases the available balance (admin operation).
func (s *PgStore) Credit(ctx context.Context, userID string, amount types.Decimal) error {
	tag, err := s.pool.Exec(ctx,
		`UPDATE wallets SET
		   available  = available + $2,
		   version    = version   + 1,
		   updated_at = now()
		 WHERE user_id=$1`,
		userID, amount.Value(),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// Auto-create wallet if it doesn't exist yet.
		_, err = s.pool.Exec(ctx,
			`INSERT INTO wallets (user_id, available, precision) VALUES ($1, $2, $3)
			 ON CONFLICT (user_id) DO UPDATE SET
			   available  = wallets.available + $2,
			   version    = wallets.version   + 1,
			   updated_at = now()`,
			userID, amount.Value(), s.precision,
		)
	}
	return err
}
