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

// UserAuthRow carries the fields needed for password login.
type UserAuthRow struct {
	UserID       string
	Email        string
	PasswordHash string
	IsAdmin      bool
}

// InsertUser creates a new user.
func InsertUser(ctx context.Context, pool *pgxpool.Pool, userID, email string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO users (user_id, email) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		userID, email,
	)
	return err
}

// CreateUserWithPassword inserts a new user with a bcrypt password hash. Returns
// an error (unique violation) if the email already exists.
func CreateUserWithPassword(ctx context.Context, pool *pgxpool.Pool, userID, email, passwordHash string) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO users (user_id, email, password_hash) VALUES ($1, $2, $3)`,
		userID, email, passwordHash,
	)
	return err
}

// GetUserByEmail fetches auth fields for password login. Returns pgx.ErrNoRows
// when the email is unknown.
func GetUserByEmail(ctx context.Context, pool *pgxpool.Pool, email string) (UserAuthRow, error) {
	var u UserAuthRow
	err := pool.QueryRow(ctx,
		`SELECT user_id, email, password_hash, is_admin FROM users WHERE email=$1`, email,
	).Scan(&u.UserID, &u.Email, &u.PasswordHash, &u.IsAdmin)
	return u, err
}

// GetUserAuth fetches auth fields by user ID (used to refresh /auth/me).
func GetUserAuth(ctx context.Context, pool *pgxpool.Pool, userID string) (UserAuthRow, error) {
	var u UserAuthRow
	err := pool.QueryRow(ctx,
		`SELECT user_id, email, password_hash, is_admin FROM users WHERE user_id=$1`, userID,
	).Scan(&u.UserID, &u.Email, &u.PasswordHash, &u.IsAdmin)
	if errors.Is(err, pgx.ErrNoRows) {
		return UserAuthRow{}, errors.New("user not found: " + userID)
	}
	return u, err
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
