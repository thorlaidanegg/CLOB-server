package engineservice

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	"github.com/thorlaidanegg/clob/events"
)

// EventPublisher reads from the engine events channel, serializes to JSON, and writes to a bus.Producer.
// In ROLE=engine: producer is KafkaProducer.
// In ROLE=all:    producer is InMemBus.
type EventPublisher struct {
	producer bus.Producer
	logger   zerolog.Logger
}

// NewEventPublisher creates an EventPublisher.
func NewEventPublisher(producer bus.Producer, logger zerolog.Logger) *EventPublisher {
	return &EventPublisher{producer: producer, logger: logger}
}

// Run reads events until evts is closed or ctx is done.
func (p *EventPublisher) Run(ctx context.Context, evts <-chan events.Event) {
	for {
		select {
		case ev, ok := <-evts:
			if !ok {
				return
			}
			payload, err := json.Marshal(ev)
			if err != nil {
				p.logger.Error().Err(err).Str("type", ev.Type()).Msg("failed to marshal event")
				continue
			}
			msg := bus.Message{
				Topic: "market-events",
				Key:   string(ev.MarketID()),
				Value: payload,
				Headers: map[string]string{
					"event-type": ev.Type(),
					"seq-num":    strconv.FormatUint(ev.SeqNum(), 10),
				},
			}
			if err := p.producer.Publish(ctx, msg); err != nil {
				p.logger.Error().Err(err).Str("type", ev.Type()).Msg("failed to publish event")
			}
		case <-ctx.Done():
			return
		}
	}
}
