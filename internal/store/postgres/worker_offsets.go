package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// WorkerOffsetRow mirrors the worker_offsets table.
type WorkerOffsetRow struct {
	WorkerName     string
	MarketID       string
	LastEventSeq   uint64
	KafkaOffset    int64
	KafkaPartition int32
}

// ListWorkerOffsets returns all offset rows for a given worker.
func ListWorkerOffsets(ctx context.Context, pool *pgxpool.Pool, workerName string) ([]WorkerOffsetRow, error) {
	rows, err := pool.Query(ctx,
		`SELECT worker_name, market_id, last_event_seq, kafka_offset, kafka_partition
		 FROM worker_offsets WHERE worker_name=$1`,
		workerName,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []WorkerOffsetRow
	for rows.Next() {
		var r WorkerOffsetRow
		if err := rows.Scan(&r.WorkerName, &r.MarketID, &r.LastEventSeq, &r.KafkaOffset, &r.KafkaPartition); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// UpsertWorkerOffsetTx updates or inserts an offset record inside a transaction.
func UpsertWorkerOffsetTx(ctx context.Context, tx pgx.Tx, workerName, marketID string, lastSeq uint64, partition int32, offset int64) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO worker_offsets (worker_name, market_id, last_event_seq, kafka_offset, kafka_partition, updated_at)
		 VALUES ($1,$2,$3,$4,$5,now())
		 ON CONFLICT (worker_name, market_id) DO UPDATE SET
		   last_event_seq  = EXCLUDED.last_event_seq,
		   kafka_offset    = EXCLUDED.kafka_offset,
		   kafka_partition = EXCLUDED.kafka_partition,
		   updated_at      = now()`,
		workerName, marketID, lastSeq, offset, partition,
	)
	return err
}
