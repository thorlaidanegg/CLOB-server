package ws

import (
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ratelimit"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
)

// ServeWS upgrades an HTTP connection to WebSocket and registers it with the hub.
func ServeWS(hub *Hub, engine client.EngineAdapter, pg *pgxpool.Pool, rdb *redis.Client, orderStore ordersstore.Store, limitRPS int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if err != nil {
			return
		}

		id := fmt.Sprintf("%d", time.Now().UnixNano())
		c := &Client{
			id:         id,
			conn:       conn,
			send:       make(chan []byte, 256),
			hub:        hub,
			engine:     engine,
			orderStore: orderStore,
			pg:         pg,
			rdb:        rdb,
			limiter:    ratelimit.NewWSLimiter(limitRPS),
		}

		// Close unauthenticated connections after 10 seconds.
		c.authTimeout = time.AfterFunc(10*time.Second, func() {
			if !c.authed {
				conn.Close(websocket.StatusPolicyViolation, "auth timeout")
			}
		})

		hub.register <- c
		ctx := r.Context()
		go c.WritePump(ctx)
		c.ReadPump(ctx)
	}
}
