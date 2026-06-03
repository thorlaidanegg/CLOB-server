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

// lockID is an arbitrary constant for the global integration-test advisory lock.
// Every RequirePostgres holds this lock for the duration of its test so that
// integration tests across packages never truncate each other's data, even though
// `go test ./...` runs package test binaries in parallel against one database.
const lockID int64 = 0xC10B7E57 // "CLOB TEST"

// RequirePostgres connects to the test database, runs migrations, acquires a
// global advisory lock, and truncates all tables for a clean slate. Skips the
// test if TEST_POSTGRES_DSN is unset. The lock and a final truncate run on cleanup.
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgstore.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("testsupport: connect: %v", err)
	}

	// Hold a session-scoped advisory lock on a dedicated connection for the whole
	// test, acquired BEFORE migrations. Other integration tests block here until
	// this one finishes, giving each test exclusive use of the database — this
	// also serializes the migration runner so concurrent CREATE TABLE statements
	// across packages don't race. pg_advisory_lock blocks rather than fails.
	lockConn, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("testsupport: acquire lock conn: %v", err)
	}
	if _, err := lockConn.Exec(context.Background(), "SELECT pg_advisory_lock($1)", lockID); err != nil {
		lockConn.Release()
		t.Fatalf("testsupport: advisory lock: %v", err)
	}

	if err := pgstore.RunMigrations(ctx, pool); err != nil {
		lockConn.Release()
		t.Fatalf("testsupport: migrations: %v", err)
	}

	TruncateAll(t, pool)
	t.Cleanup(func() {
		TruncateAll(t, pool)
		lockConn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", lockID)
		lockConn.Release() // ends the session, dropping the lock even if unlock failed
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
		`TRUNCATE wallets, positions, orders, markets, users, api_keys,
		          worker_offsets, trades, dead_letter_events, book_snapshots CASCADE`)
	if err != nil {
		t.Fatalf("testsupport: truncate: %v", err)
	}
}
