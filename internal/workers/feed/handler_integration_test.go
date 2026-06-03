package feed_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/rs/zerolog"
	"github.com/thorlaidanegg/clob/events"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ws"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/thorlaidanegg/clob-server/internal/workers"
	"github.com/thorlaidanegg/clob-server/internal/workers/feed"
)

// TestFeedHandler_RoutesToChannels verifies that feed routes events to the
// expected WebSocket channels. It shares one hub between a real WS subscriber
// (via ServeWS) and the feed handler, so delivery is observed end-to-end.
func TestFeedHandler_RoutesToChannels(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	rdb := testsupport.RequireMiniRedis(t)
	ctx := context.Background()

	// Seed a key so the WS client can authenticate.
	if err := pgstore.InsertUser(ctx, pool, "alice", "alice@feed.local"); err != nil {
		t.Fatal(err)
	}
	full, hash, prefix, err := auth.GenerateKey("clob_live")
	if err != nil {
		t.Fatal(err)
	}
	if err := pgstore.InsertAPIKey(ctx, pool, pgstore.APIKeyRow{
		UserID: "alice", KeyHash: hash, KeyPrefix: prefix, Name: "feed",
		Scopes: []string{"feed:read"}, Tier: "standard", RateLimit: 300,
	}); err != nil {
		t.Fatal(err)
	}

	hub := ws.NewHub()
	go hub.Run()
	srv := httptest.NewServer(ws.ServeWS(hub, &testsupport.FakeEngine{}, pool, rdb, ordersstore.NewPgStore(pool), 50))
	defer srv.Close()
	url := "ws" + strings.TrimPrefix(srv.URL, "http")

	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	readType := func() string {
		_, data, err := conn.Read(dctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var m map[string]any
		_ = json.Unmarshal(data, &m)
		if s, ok := m["type"].(string); ok {
			return s
		}
		return ""
	}

	// Authenticate, then subscribe to the trades channel.
	if err := conn.Write(dctx, websocket.MessageText, []byte(`{"type":"auth","apiKey":"`+full+`"}`)); err != nil {
		t.Fatal(err)
	}
	if got := readType(); got != "auth_ok" {
		t.Fatalf("auth response = %q, want auth_ok", got)
	}
	if err := conn.Write(dctx, websocket.MessageText, []byte(`{"type":"subscribe","channel":"trades:BTC-USD"}`)); err != nil {
		t.Fatal(err)
	}
	if got := readType(); got != "subscribed" {
		t.Fatalf("subscribe response = %q, want subscribed", got)
	}

	// The feed handler shares the same hub. A TradeExecuted must reach trades:BTC-USD.
	h := feed.New(hub, rdb, zerolog.Nop())
	payload := []byte(`{"type":"trade_executed","tradeID":"t-1"}`)
	if err := h.HandleEvent(ctx, nil, workers.EventEnvelope{
		EventType: events.TypeTradeExecuted, MarketID: "BTC-USD", Raw: payload,
	}); err != nil {
		t.Fatal(err)
	}

	_, data, err := conn.Read(dctx)
	if err != nil {
		t.Fatalf("read trade: %v", err)
	}
	if string(data) != string(payload) {
		t.Errorf("delivered %q, want %q", data, payload)
	}
}
