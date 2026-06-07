package ws

import (
	"context"
	"encoding/json"
	"os"

	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/bus"
	"github.com/thorlaidanegg/clob/events"
)

// Broadcaster reads from the event bus and fans out to WebSocket clients.
type Broadcaster struct {
	consumer   bus.Consumer
	hub        *Hub
	instanceID string
	logger     zerolog.Logger
}

// NewBroadcaster creates a Broadcaster.
func NewBroadcaster(consumer bus.Consumer, hub *Hub, logger zerolog.Logger) *Broadcaster {
	id, _ := os.Hostname()
	if id == "" {
		id = "unknown"
	}
	return &Broadcaster{consumer: consumer, hub: hub, instanceID: id, logger: logger}
}

// Run subscribes to market-events and routes them to hub channels.
func (b *Broadcaster) Run(ctx context.Context) {
	groupID := "gateway-feed-" + b.instanceID
	b.consumer.Subscribe("market-events", groupID)

	for {
		msg, err := b.consumer.Poll(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.logger.Error().Err(err).Msg("broadcaster: poll error")
			continue
		}

		eventType := msg.Headers["event-type"]
		marketID := msg.Key

		emit := func(channel string) {
			b.hub.Broadcast(channel, envelope(channel, eventType, msg.Value))
		}

		switch eventType {
		case events.TypeDepthUpdate:
			emit("depth:" + marketID)
		case events.TypeTradeExecuted:
			emit("trades:" + marketID)
		case events.TypeTradeFill:
			var ev events.TradeFill
			if err := json.Unmarshal(msg.Value, &ev); err == nil {
				uid := string(ev.UserID)
				emit("orders:" + uid)
				emit("portfolio:" + uid)
			}
		case events.TypeOrderRested, events.TypeOrderCanceled,
			events.TypeOrderRejected, events.TypeOrderExpired, events.TypeOrderAccepted:
			var base struct {
				UserID string `json:"userID"`
			}
			if err := json.Unmarshal(msg.Value, &base); err == nil && base.UserID != "" {
				emit("orders:" + base.UserID)
			}
		case events.TypeMarketHalted, events.TypeMarketResumed:
			emit("markets")
		case events.TypeAuctionOpened, events.TypeAuctionCleared:
			emit("status:" + marketID)
			emit("markets")
		}
	}
}

// envelope wraps a raw event in { channel, type, data } so browser clients can
// route it (the raw event JSON carries no channel/type discriminator).
func envelope(channel, eventType string, raw []byte) []byte {
	b, _ := json.Marshal(struct {
		Channel string          `json:"channel"`
		Type    string          `json:"type"`
		Data    json.RawMessage `json:"data"`
	}{Channel: channel, Type: eventType, Data: raw})
	return b
}
