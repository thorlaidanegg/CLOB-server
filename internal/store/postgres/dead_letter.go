package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DeadLetterRow mirrors the dead_letter_events table.
type DeadLetterRow struct {
	WorkerName string
	MarketID   string
	SeqNum     uint64
	EventType  string
	Payload    []byte
	Error      string
}

const insertDeadLetterSQL = `INSERT INTO dead_letter_events
	(worker_name, market_id, seq_num, event_type, payload, error)
	VALUES ($1,$2,$3,$4,$5,$6)`

// InsertDeadLetter records an event a worker could not process after retries, so
// it can be inspected and replayed manually instead of blocking the stream.
func InsertDeadLetter(ctx context.Context, pool *pgxpool.Pool, r DeadLetterRow) error {
	_, err := pool.Exec(ctx, insertDeadLetterSQL,
		r.WorkerName, r.MarketID, int64(r.SeqNum), r.EventType, r.Payload, r.Error)
	return err
}

// InsertDeadLetterTx records a dead-letter row inside an existing transaction.
func InsertDeadLetterTx(ctx context.Context, tx pgx.Tx, r DeadLetterRow) error {
	_, err := tx.Exec(ctx, insertDeadLetterSQL,
		r.WorkerName, r.MarketID, int64(r.SeqNum), r.EventType, r.Payload, r.Error)
	return err
}

// CountDeadLetters returns how many dead-letter rows exist for a worker (for tests/ops).
func CountDeadLetters(ctx context.Context, pool *pgxpool.Pool, workerName string) (int, error) {
	var n int
	err := pool.QueryRow(ctx,
		`SELECT count(*) FROM dead_letter_events WHERE worker_name=$1`, workerName,
	).Scan(&n)
	return n, err
}
