package ws

import (
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/client"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ratelimit"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
)

// ServeWS upgrades an HTTP connection to WebSocket and registers it with the hub.
// Browsers authenticate via the JWT session cookie at upgrade; bots may instead
// send an {"type":"auth","apiKey":...} frame within 10 seconds.
func ServeWS(hub *Hub, engine client.EngineAdapter, pg *pgxpool.Pool, rdb *redis.Client, orderStore ordersstore.Store, jwtSecret string, limitRPS int) http.HandlerFunc {
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

		// Cookie auth: if the browser presented a valid session, mark the client
		// authed before registering. The hub auto-subscribes it to its personal
		// channels (orders/portfolio) and emits auth_ok once registered.
		if token := auth.ReadSessionToken(r); token != "" {
			if claims, perr := auth.ParseSession(jwtSecret, token); perr == nil {
				c.authed = true
				c.userID = claims.UserID
			}
		}

		// Bots that didn't cookie-auth have 10s to send an auth frame.
		if !c.authed {
			c.authTimeout = time.AfterFunc(10*time.Second, func() {
				if !c.authed {
					conn.Close(websocket.StatusPolicyViolation, "auth timeout")
				}
			})
		}

		hub.register <- c
		ctx := r.Context()
		go c.WritePump(ctx)
		c.ReadPump(ctx)
	}
}
