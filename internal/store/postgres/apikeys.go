package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// APIKeyRow mirrors the api_keys table. JSON tags are camelCase to match the
// API contract (see api/openapi.yaml); KeyHash is never serialized to clients.
type APIKeyRow struct {
	ID         string     `json:"id"`
	UserID     string     `json:"userID"`
	KeyHash    string     `json:"-"`
	KeyPrefix  string     `json:"keyPrefix"`
	Name       string     `json:"name"`
	Scopes     []string   `json:"scopes"`
	Tier       string     `json:"tier"`
	RateLimit  int        `json:"rateLimit"`
	LastUsedAt *time.Time `json:"lastUsedAt"`
	ExpiresAt  *time.Time `json:"expiresAt"`
	Revoked    bool       `json:"revoked"`
	CreatedAt  *time.Time `json:"createdAt"`
}

// InsertAPIKey creates a new API key record.
func InsertAPIKey(ctx context.Context, pool *pgxpool.Pool, k APIKeyRow) error {
	_, err := pool.Exec(ctx,
		`INSERT INTO api_keys (user_id, key_hash, key_prefix, name, scopes, tier, rate_limit, expires_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		k.UserID, k.KeyHash, k.KeyPrefix, k.Name, k.Scopes, k.Tier, k.RateLimit, k.ExpiresAt,
	)
	return err
}

// GetAPIKeyByHash fetches a key record by its SHA-256 hash.
func GetAPIKeyByHash(ctx context.Context, pool *pgxpool.Pool, hash string) (APIKeyRow, error) {
	var k APIKeyRow
	err := pool.QueryRow(ctx,
		`SELECT id, user_id, key_hash, key_prefix, COALESCE(name,''), scopes,
		        tier, rate_limit, last_used_at, expires_at, revoked
		 FROM api_keys WHERE key_hash=$1`,
		hash,
	).Scan(&k.ID, &k.UserID, &k.KeyHash, &k.KeyPrefix, &k.Name, &k.Scopes,
		&k.Tier, &k.RateLimit, &k.LastUsedAt, &k.ExpiresAt, &k.Revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return APIKeyRow{}, errors.New("api key not found")
	}
	return k, err
}

// UpdateLastUsed sets the last_used_at timestamp for a key.
func UpdateLastUsed(ctx context.Context, pool *pgxpool.Pool, hash string) {
	now := time.Now()
	pool.Exec(ctx,
		`UPDATE api_keys SET last_used_at=$2 WHERE key_hash=$1`, hash, now,
	)
}

// RevokeAPIKey sets the revoked flag.
func RevokeAPIKey(ctx context.Context, pool *pgxpool.Pool, keyID, ownerUserID string) error {
	tag, err := pool.Exec(ctx,
		`UPDATE api_keys SET revoked=TRUE WHERE id=$1 AND user_id=$2`,
		keyID, ownerUserID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return errors.New("api key not found or not owned by user")
	}
	return nil
}

// ListAPIKeysByUser returns all non-revoked keys for a user.
func ListAPIKeysByUser(ctx context.Context, pool *pgxpool.Pool, userID string) ([]APIKeyRow, error) {
	rows, err := pool.Query(ctx,
		`SELECT id, user_id, key_hash, key_prefix, COALESCE(name,''), scopes,
		        tier, rate_limit, last_used_at, expires_at, revoked, created_at
		 FROM api_keys WHERE user_id=$1 ORDER BY created_at DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []APIKeyRow
	for rows.Next() {
		var k APIKeyRow
		if err := rows.Scan(&k.ID, &k.UserID, &k.KeyHash, &k.KeyPrefix, &k.Name, &k.Scopes,
			&k.Tier, &k.RateLimit, &k.LastUsedAt, &k.ExpiresAt, &k.Revoked, &k.CreatedAt); err != nil {
			return nil, err
		}
		result = append(result, k)
	}
	return result, rows.Err()
}

// AdminKeyExists returns true if any admin-tier key exists.
func AdminKeyExists(ctx context.Context, pool *pgxpool.Pool) (bool, error) {
	var exists bool
	err := pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM api_keys WHERE tier='admin' AND revoked=FALSE)`,
	).Scan(&exists)
	return exists, err
}
