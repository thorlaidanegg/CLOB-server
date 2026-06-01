// Package testsupport provides shared helpers for integration tests that need
// real infrastructure. Integration tests are gated behind the TEST_POSTGRES_DSN
// environment variable and skip cleanly when it is not set.
package testsupport

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
)

// RequirePostgres connects to the test database, runs migrations, and truncates
// all tables for a clean slate. Skips the test if TEST_POSTGRES_DSN is unset.
//
// Example:
//
//	export TEST_POSTGRES_DSN="postgres://clob:clob@localhost:5432/clob_test?sslmode=disable"
//	go test ./...
func RequirePostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN not set — skipping Postgres integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgstore.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("testsupport: connect: %v", err)
	}
	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		t.Fatalf("testsupport: migrations: %v", err)
	}

	TruncateAll(t, pool)
	t.Cleanup(func() {
		TruncateAll(t, pool)
		pool.Close()
	})
	return pool
}

// TruncateAll wipes every table so each test starts from an empty database.
func TruncateAll(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := pool.Exec(ctx,
		`TRUNCATE wallets, positions, orders, markets, users, api_keys, worker_offsets, trades CASCADE`)
	if err != nil {
		t.Fatalf("testsupport: truncate: %v", err)
	}
}
