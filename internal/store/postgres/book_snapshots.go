package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BookSnapshotRow mirrors the book_snapshots table.
type BookSnapshotRow struct {
	MarketID       string
	LastEventSeq   uint64
	KafkaOffset    int64
	KafkaPartition int32
	State          []byte
}

// UpsertBookSnapshotTx writes (or replaces) a market's latest book snapshot inside
// an existing transaction, so it commits atomically with the worker's offset.
func UpsertBookSnapshotTx(ctx context.Context, tx pgx.Tx, r BookSnapshotRow) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO book_snapshots
		   (market_id, last_event_seq, kafka_offset, kafka_partition, state, updated_at)
		 VALUES ($1,$2,$3,$4,$5,now())
		 ON CONFLICT (market_id) DO UPDATE SET
		   last_event_seq  = EXCLUDED.last_event_seq,
		   kafka_offset    = EXCLUDED.kafka_offset,
		   kafka_partition = EXCLUDED.kafka_partition,
		   state           = EXCLUDED.state,
		   updated_at      = now()`,
		r.MarketID, int64(r.LastEventSeq), r.KafkaOffset, r.KafkaPartition, r.State,
	)
	return err
}

// GetBookSnapshot returns the latest snapshot for a market. ok is false if none exists.
func GetBookSnapshot(ctx context.Context, pool *pgxpool.Pool, marketID string) (BookSnapshotRow, bool, error) {
	var r BookSnapshotRow
	var seq int64
	err := pool.QueryRow(ctx,
		`SELECT market_id, last_event_seq, kafka_offset, kafka_partition, state
		 FROM book_snapshots WHERE market_id=$1`, marketID,
	).Scan(&r.MarketID, &seq, &r.KafkaOffset, &r.KafkaPartition, &r.State)
	if errors.Is(err, pgx.ErrNoRows) {
		return BookSnapshotRow{}, false, nil
	}
	if err != nil {
		return BookSnapshotRow{}, false, err
	}
	r.LastEventSeq = uint64(seq)
	return r, true, nil
}

// ListBookSnapshots returns the latest snapshot for every market, used by the
// snapshot worker to seed its in-memory state on startup.
func ListBookSnapshots(ctx context.Context, pool *pgxpool.Pool) ([]BookSnapshotRow, error) {
	rows, err := pool.Query(ctx,
		`SELECT market_id, last_event_seq, kafka_offset, kafka_partition, state FROM book_snapshots`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []BookSnapshotRow
	for rows.Next() {
		var r BookSnapshotRow
		var seq int64
		if err := rows.Scan(&r.MarketID, &seq, &r.KafkaOffset, &r.KafkaPartition, &r.State); err != nil {
			return nil, err
		}
		r.LastEventSeq = uint64(seq)
		out = append(out, r)
	}
	return out, rows.Err()
}
