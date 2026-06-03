// Package booksnapshot implements the checkpoint worker. It folds the
// market-events log into a per-market resting-book state (see internal/bookstate)
// and persists it, bounding how far crash recovery has to replay.
package booksnapshot

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bookstate"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
)

// Handler folds events into per-market BookState and checkpoints each one.
// It is driven by a single WorkerRunner goroutine, so the states map needs no lock.
type Handler struct {
	states map[string]*bookstate.BookState
	log    zerolog.Logger
}

// New creates a snapshot handler with empty state. Call LoadSnapshots before Run
// to seed state from the last persisted checkpoints.
func New(log zerolog.Logger) *Handler {
	return &Handler{states: make(map[string]*bookstate.BookState), log: log}
}

// LoadSnapshots seeds in-memory state from persisted checkpoints. The latest
// checkpoint per market shares its event seq with worker_offsets (both written in
// one transaction per event), so after seeding the WorkerRunner resumes exactly
// where the state left off and re-delivered events fold idempotently.
func (h *Handler) LoadSnapshots(ctx context.Context, pool *pgxpool.Pool) error {
	rows, err := pgstore.ListBookSnapshots(ctx, pool)
	if err != nil {
		return err
	}
	for _, r := range rows {
		st := bookstate.New()
		if err := json.Unmarshal(r.State, st); err != nil {
			h.log.Error().Err(err).Str("market", r.MarketID).Msg("booksnapshot: corrupt checkpoint, starting market fresh")
			st = bookstate.New()
		}
		h.states[r.MarketID] = st
	}
	return nil
}

// HandleEvent folds one event and upserts the market's checkpoint in the same
// transaction the WorkerRunner uses for the offset, keeping book_snapshots and
// worker_offsets in lockstep.
func (h *Handler) HandleEvent(ctx context.Context, tx pgx.Tx, env workers.EventEnvelope) error {
	st, ok := h.states[env.MarketID]
	if !ok {
		st = bookstate.New()
		h.states[env.MarketID] = st
	}
	st.Apply(env.Event)

	blob, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return pgstore.UpsertBookSnapshotTx(ctx, tx, pgstore.BookSnapshotRow{
		MarketID:       env.MarketID,
		LastEventSeq:   st.LastEventSeq,
		KafkaOffset:    env.Offset,
		KafkaPartition: env.Partition,
		State:          blob,
	})
}
