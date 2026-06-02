package workers_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob/types"
)

// alwaysFailHandler fails every event, forcing the dead-letter path.
type alwaysFailHandler struct{}

func (alwaysFailHandler) HandleEvent(_ context.Context, _ pgx.Tx, _ workers.EventEnvelope) error {
	return errors.New("boom")
}

func TestWorkerRunner_DeadLettersPoisonEvent(t *testing.T) {
	pool := testsupport.RequirePostgres(t)

	b := bus.NewInMemBus()
	consumer := b.NewConsumer()
	runner := workers.NewWorkerRunner("testworker", "market-events", pool, consumer, alwaysFailHandler{}, zerolog.Nop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runner.Run(ctx)

	// Publish a well-formed event the handler will reject.
	fill := events.TradeFill{
		Base: events.NewBase(1, time.Now().UnixNano(), "BTC-USD"),
		OrderID: "ord_x", UserID: "alice", Side: types.Bid,
		Price: types.MustDecimal("100.00", 2), FilledQty: types.MustDecimal("1.00", 2),
		RemainQty: types.MustDecimal("0.00", 2),
	}
	payload, _ := json.Marshal(fill)
	b.Publish(ctx, bus.Message{
		Topic: "market-events", Key: "BTC-USD", Value: payload,
		Headers: map[string]string{"event-type": events.TypeTradeFill, "seq-num": "1"},
	})

	// After ~3 retries the event should land in dead_letter_events.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		n, err := pgstore.CountDeadLetters(context.Background(), pool, "testworker")
		if err == nil && n == 1 {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("poison event was never dead-lettered")
}
