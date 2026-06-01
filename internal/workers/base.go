package workers

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob/events"
)

// EventEnvelope wraps a deserialized event with bus metadata.
type EventEnvelope struct {
	Event     events.Event
	Raw       []byte
	SeqNum    uint64
	MarketID  string
	EventType string
	Partition int32
	Offset    int64
}

// Handler processes a single event inside a transaction.
type Handler interface {
	HandleEvent(ctx context.Context, tx pgx.Tx, env EventEnvelope) error
}

// eventRegistry maps event type strings to factory functions for JSON deserialization.
var eventRegistry = map[string]func() events.Event{
	events.TypeTradeFill:     func() events.Event { return &events.TradeFill{} },
	events.TypeTradeExecuted: func() events.Event { return &events.TradeExecuted{} },
	events.TypeOrderAccepted: func() events.Event { return &events.OrderAccepted{} },
	events.TypeOrderRested:   func() events.Event { return &events.OrderRested{} },
	events.TypeOrderCanceled: func() events.Event { return &events.OrderCanceled{} },
	events.TypeOrderRejected: func() events.Event { return &events.OrderRejected{} },
	events.TypeOrderExpired:  func() events.Event { return &events.OrderExpired{} },
	events.TypeDepthUpdate:   func() events.Event { return &events.DepthUpdate{} },
	events.TypeMarketHalted:  func() events.Event { return &events.MarketHalted{} },
	events.TypeMarketResumed: func() events.Event { return &events.MarketResumed{} },
}

func deserializeEvent(eventType string, raw []byte) (events.Event, error) {
	factory, ok := eventRegistry[eventType]
	if !ok {
		return nil, nil // unknown type: caller logs and skips
	}
	ev := factory()
	return ev, json.Unmarshal(raw, ev)
}

// WorkerRunner is the base event-processing loop for all workers.
type WorkerRunner struct {
	workerName string
	topic      string
	pool       *pgxpool.Pool
	consumer   bus.Consumer
	handler    Handler
	logger     zerolog.Logger

	lastSeqs map[string]uint64 // marketID → last processed seq
}

// NewWorkerRunner creates a WorkerRunner. topic is the Kafka topic to subscribe to.
func NewWorkerRunner(name, topic string, pool *pgxpool.Pool, consumer bus.Consumer, handler Handler, log zerolog.Logger) *WorkerRunner {
	return &WorkerRunner{
		workerName: name,
		topic:      topic,
		pool:       pool,
		consumer:   consumer,
		handler:    handler,
		logger:     log,
		lastSeqs:   make(map[string]uint64),
	}
}

// Run is the main event loop. Blocks until ctx is done.
func (w *WorkerRunner) Run(ctx context.Context) {
	// Subscribe to the topic (no-op for InMemBus, required for Kafka).
	w.consumer.Subscribe(w.topic, w.workerName)

	// Load saved offsets and seek Kafka to the minimum offset per partition.
	offsets, err := pgstore.ListWorkerOffsets(ctx, w.pool, w.workerName)
	if err != nil {
		w.logger.Error().Err(err).Msg("worker: failed to load offsets")
	}

	// Build last-seq map and find minimum offset per partition.
	partitionMin := make(map[int32]int64)
	for _, o := range offsets {
		w.lastSeqs[o.MarketID] = o.LastEventSeq
		if existing, ok := partitionMin[o.KafkaPartition]; !ok || o.KafkaOffset < existing {
			partitionMin[o.KafkaPartition] = o.KafkaOffset
		}
	}
	for p, offset := range partitionMin {
		w.consumer.SeekToOffset(p, offset)
	}

	// Graceful shutdown: on exit, flush the final committed offset and close the
	// consumer so the group rebalances promptly instead of waiting for a session timeout.
	defer w.shutdown()

	for {
		msg, err := w.consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				w.logger.Info().Str("worker", w.workerName).Msg("worker: context canceled, shutting down")
				return
			}
			w.logger.Error().Err(err).Msg("worker: poll error")
			continue
		}

		eventType := msg.Headers["event-type"]
		marketID := msg.Key
		seqNum := uint64(0)
		if s, err := strconv.ParseUint(msg.Headers["seq-num"], 10, 64); err == nil {
			seqNum = s
		}

		ev, err := deserializeEvent(eventType, msg.Value)
		if ev == nil {
			// Unknown event type — skip gracefully.
			w.consumer.Commit(ctx, msg)
			continue
		}
		if err != nil {
			w.logger.Error().Err(err).Str("type", eventType).Msg("worker: deserialize error")
			w.consumer.Commit(ctx, msg)
			continue
		}

		// Idempotency check.
		if seqNum > 0 && seqNum <= w.lastSeqs[marketID] {
			w.consumer.Commit(ctx, msg)
			continue
		}

		env := EventEnvelope{
			Event:     ev,
			Raw:       msg.Value,
			SeqNum:    seqNum,
			MarketID:  marketID,
			EventType: eventType,
			Partition: msg.Partition,
			Offset:    msg.Offset,
		}

		// Run handler and offset update in a single transaction.
		if err := w.runTransaction(ctx, env); err != nil {
			w.logger.Error().Err(err).Str("type", eventType).Uint64("seq", seqNum).Msg("worker: transaction failed")
			continue
		}

		w.lastSeqs[marketID] = seqNum
		w.consumer.Commit(ctx, msg)
	}
}

// shutdown flushes the final committed offset and closes the consumer.
// Called on Run exit. Uses a fresh short-lived context because the worker's
// own context is already canceled by the time we get here.
func (w *WorkerRunner) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.consumer.Commit(ctx, bus.Message{}); err != nil {
		w.logger.Warn().Err(err).Str("worker", w.workerName).Msg("worker: final commit failed")
	}
	if err := w.consumer.Close(); err != nil {
		w.logger.Warn().Err(err).Str("worker", w.workerName).Msg("worker: consumer close failed")
	}
}

func (w *WorkerRunner) runTransaction(ctx context.Context, env EventEnvelope) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := w.handler.HandleEvent(ctx, tx, env); err != nil {
		return err
	}

	if err := pgstore.UpsertWorkerOffsetTx(ctx, tx, w.workerName, env.MarketID, env.SeqNum, env.Partition, env.Offset); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
