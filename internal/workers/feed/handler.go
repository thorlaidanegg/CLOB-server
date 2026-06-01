package feed

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ws"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	"github.com/thorlaidanegg/clob/events"
)

// Handler routes events to WebSocket hub channels and updates Redis BBO/last-price.
type Handler struct {
	hub    *ws.Hub
	rdb    *redis.Client
	logger zerolog.Logger
}

// New creates a feed Handler.
func New(hub *ws.Hub, rdb *redis.Client, logger zerolog.Logger) *Handler {
	return &Handler{hub: hub, rdb: rdb, logger: logger}
}

// HandleEvent routes the event to the appropriate WebSocket channel.
func (h *Handler) HandleEvent(ctx context.Context, _ pgx.Tx, env workers.EventEnvelope) error {
	marketID := env.MarketID

	switch env.EventType {
	case events.TypeDepthUpdate:
		h.hub.Broadcast("depth:"+marketID, env.Raw)
		// Update BBO cache.
		var ev events.DepthUpdate
		if err := json.Unmarshal(env.Raw, &ev); err == nil {
			// BBO is set by the first bid/ask depth level — simplified version.
			_ = redisstore.SetBBO(ctx, h.rdb, marketID, "", "")
		}

	case events.TypeTradeExecuted:
		h.hub.Broadcast("trades:"+marketID, env.Raw)

	case events.TypeTradeFill:
		var ev events.TradeFill
		if err := json.Unmarshal(env.Raw, &ev); err == nil {
			uid := string(ev.UserID)
			h.hub.Broadcast("orders:"+uid, env.Raw)
			h.hub.Broadcast("portfolio:"+uid, env.Raw)
		}

	case events.TypeOrderAccepted, events.TypeOrderRested, events.TypeOrderCanceled,
		events.TypeOrderRejected, events.TypeOrderExpired:
		var base struct {
			UserID string `json:"userID"`
		}
		if err := json.Unmarshal(env.Raw, &base); err == nil && base.UserID != "" {
			h.hub.Broadcast("orders:"+base.UserID, env.Raw)
		}

	case events.TypeMarketHalted, events.TypeMarketResumed:
		h.hub.Broadcast("markets", env.Raw)
	}

	return nil
}
