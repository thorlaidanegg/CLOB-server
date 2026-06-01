package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// UserRow mirrors the users table.
type UserRow struct {
	UserID string
	Email  string
}

// InsertUser creates a new user.
func InsertUser(ctx context.Context, pool *pgxpool.Pool, userID, email string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO users (user_id, email) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		userID, email,
	)
	return err
}

// GetUser fetches a user by ID.
func GetUser(ctx context.Context, pool *pgxpool.Pool, userID string) (UserRow, error) {
	var u UserRow
	err := pool.QueryRow(ctx,
		`SELECT user_id, email FROM users WHERE user_id=$1`, userID,
	).Scan(&u.UserID, &u.Email)
	if errors.Is(err, pgx.ErrNoRows) {
		return UserRow{}, errors.New("user not found: " + userID)
	}
	return u, err
}
