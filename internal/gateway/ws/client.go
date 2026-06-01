package ws

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/normalizer"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ratelimit"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
)

// Client represents a single WebSocket connection.
type Client struct {
	id          string
	conn        *websocket.Conn
	userID      string
	authed      bool
	authTimeout *time.Timer
	send        chan []byte
	hub         *Hub
	engine      client.EngineAdapter
	orderStore  ordersstore.Store
	pg          *pgxpool.Pool
	rdb         *redis.Client
	limiter     *ratelimit.WSLimiter
}

// inbound message types
type wsMsg struct {
	Type string `json:"type"`

	// auth
	APIKey string `json:"apiKey,omitempty"`

	// subscribe / unsubscribe
	Channel string `json:"channel,omitempty"`

	// place_order
	MarketID   string `json:"marketID,omitempty"`
	Side       string `json:"side,omitempty"`
	OrderType  string `json:"orderType,omitempty"`
	Price      string `json:"price,omitempty"`
	StopPrice  string `json:"stopPrice,omitempty"`
	Qty        string `json:"qty,omitempty"`
	DisplayQty string `json:"displayQty,omitempty"`
	TIF        string `json:"tif,omitempty"`
	ExpireAt   string `json:"expireAt,omitempty"`
	STPMode    string `json:"stpMode,omitempty"`

	// cancel_order
	OrderID string `json:"orderID,omitempty"`
}

// ReadPump handles all inbound messages for this connection.
func (c *Client) ReadPump(ctx context.Context) {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close(websocket.StatusNormalClosure, "bye")
	}()

	for {
		_, data, err := c.conn.Read(ctx)
		if err != nil {
			return
		}

		var msg wsMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			c.sendJSON(map[string]string{"type": "error", "message": "invalid JSON"})
			continue
		}

		// Auth must be first before any other message type.
		if !c.authed {
			if msg.Type != "auth" {
				c.sendJSON(map[string]string{"type": "error", "message": "send auth first"})
				c.conn.Close(websocket.StatusPolicyViolation, "unauthenticated")
				return
			}
			c.handleAuth(ctx, msg.APIKey)
			continue
		}

		if !c.limiter.Allow() {
			c.sendJSON(map[string]string{"type": "error", "message": "rate limit exceeded"})
			continue
		}

		switch msg.Type {
		case "subscribe":
			c.hub.Subscribe(c.id, msg.Channel)
			c.sendJSON(map[string]string{"type": "subscribed", "channel": msg.Channel})
		case "unsubscribe":
			c.hub.Unsubscribe(c.id, msg.Channel)
			c.sendJSON(map[string]string{"type": "unsubscribed", "channel": msg.Channel})
		case "place_order":
			c.handlePlaceOrder(ctx, msg)
		case "cancel_order":
			c.handleCancelOrder(ctx, msg)
		default:
			c.sendJSON(map[string]string{"type": "error", "message": "unknown message type: " + msg.Type})
		}
	}
}

// WritePump drains the send channel to the WebSocket connection.
func (c *Client) WritePump(ctx context.Context) {
	for payload := range c.send {
		if err := c.conn.Write(ctx, websocket.MessageText, payload); err != nil {
			return
		}
	}
}

func (c *Client) handleAuth(ctx context.Context, apiKey string) {
	if apiKey == "" {
		c.sendJSON(map[string]string{"type": "auth_error", "message": "apiKey required"})
		c.conn.Close(websocket.StatusPolicyViolation, "no api key")
		return
	}

	ac, err := auth.ValidateKey(ctx, apiKey, c.pg, c.rdb)
	if err != nil {
		c.sendJSON(map[string]string{"type": "auth_error", "message": err.Error()})
		c.conn.Close(websocket.StatusPolicyViolation, "invalid key")
		return
	}

	c.authed = true
	c.userID = ac.UserID
	if c.authTimeout != nil {
		c.authTimeout.Stop()
	}

	// Auto-subscribe to personal channels on auth.
	c.hub.Subscribe(c.id, "orders:"+c.userID)
	c.hub.Subscribe(c.id, "portfolio:"+c.userID)

	c.sendJSON(map[string]interface{}{
		"type":   "auth_ok",
		"userID": c.userID,
	})
}

func (c *Client) handlePlaceOrder(ctx context.Context, msg wsMsg) {
	mkt, err := pgstore.GetMarket(ctx, c.pg, msg.MarketID)
	if err != nil {
		c.sendJSON(map[string]string{"type": "error", "message": "market not found: " + msg.MarketID})
		return
	}

	built, err := normalizer.BuildPlaceRequest(c.userID, normalizer.OrderParams{
		MarketID:   msg.MarketID,
		Side:       msg.Side,
		OrderType:  msg.OrderType,
		Price:      msg.Price,
		StopPrice:  msg.StopPrice,
		Qty:        msg.Qty,
		DisplayQty: msg.DisplayQty,
		TIF:        msg.TIF,
		ExpireAt:   msg.ExpireAt,
		STPMode:    msg.STPMode,
	}, mkt)
	if err != nil {
		c.sendJSON(map[string]string{"type": "error", "message": err.Error()})
		return
	}

	if err := c.orderStore.InsertOrder(ctx, built.OrderRow); err != nil {
		c.sendJSON(map[string]string{"type": "error", "message": "failed to record order"})
		return
	}

	resp, err := c.engine.PlaceOrder(ctx, built.EngineReq)
	if err != nil {
		c.sendJSON(map[string]string{"type": "error", "message": err.Error()})
		return
	}

	c.sendJSON(map[string]interface{}{
		"type":    "order_accepted",
		"orderID": resp.OrderID,
		"seqNum":  resp.SeqNum,
		"status":  resp.Status,
	})
}

func (c *Client) handleCancelOrder(ctx context.Context, msg wsMsg) {
	if msg.OrderID == "" {
		c.sendJSON(map[string]string{"type": "error", "message": "orderID required"})
		return
	}

	if err := c.engine.CancelOrder(ctx, msg.OrderID, c.userID); err != nil {
		c.sendJSON(map[string]string{"type": "error", "message": err.Error()})
		return
	}

	c.sendJSON(map[string]interface{}{
		"type":    "cancel_accepted",
		"orderID": msg.OrderID,
	})
}

func (c *Client) sendJSON(v any) {
	b, _ := json.Marshal(v)
	select {
	case c.send <- b:
	default:
	}
}
