package ws

import (
	"context"
	"encoding/json"
	"time"

	"github.com/coder/websocket"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ratelimit"
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
	limiter     *ratelimit.WSLimiter
}

type wsMsg struct {
	Type    string          `json:"type"`
	Channel string          `json:"channel,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// ReadPump handles inbound messages from the WebSocket connection.
func (c *Client) ReadPump(ctx context.Context, pg interface{ ValidateKey(ctx context.Context, key string) (auth.AuthContext, error) }) {
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
			continue
		}

		if !c.authed && msg.Type != "auth" {
			c.sendJSON(map[string]string{"type": "error", "message": "not authenticated"})
			return
		}

		if !c.limiter.Allow() {
			c.sendJSON(map[string]string{"type": "error", "message": "rate limit exceeded"})
			continue
		}

		switch msg.Type {
		case "subscribe":
			c.hub.Subscribe(c.id, msg.Channel)
		case "unsubscribe":
			c.hub.Unsubscribe(c.id, msg.Channel)
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

func (c *Client) sendJSON(v any) {
	b, _ := json.Marshal(v)
	select {
	case c.send <- b:
	default:
	}
}
