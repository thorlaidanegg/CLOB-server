package ws_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/ws"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedWSKey(t *testing.T, pool *pgxpool.Pool, userID string) string {
	t.Helper()
	ctx := context.Background()
	if err := pgstore.InsertUser(ctx, pool, userID, userID+"@ws.local"); err != nil {
		t.Fatal(err)
	}
	full, hash, prefix, err := auth.GenerateKey("clob_live")
	if err != nil {
		t.Fatal(err)
	}
	if err := pgstore.InsertAPIKey(ctx, pool, pgstore.APIKeyRow{
		UserID: userID, KeyHash: hash, KeyPrefix: prefix, Name: "ws",
		Scopes: []string{"trade:write", "feed:read"}, Tier: "standard", RateLimit: 300,
	}); err != nil {
		t.Fatal(err)
	}
	return full
}

func startWSServer(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	rdb := testsupport.RequireMiniRedis(t)
	hub := ws.NewHub()
	go hub.Run()
	handler := ws.ServeWS(hub, &testsupport.FakeEngine{}, pool, rdb, ordersstore.NewPgStore(pool), 50)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func dialAndExchange(t *testing.T, url, send string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	if err := conn.Write(ctx, websocket.MessageText, []byte(send)); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var resp map[string]any
	if err := json.Unmarshal(data, &resp); err != nil {
		t.Fatalf("unmarshal %q: %v", data, err)
	}
	return resp
}

func TestWS_AuthSuccess(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	url := startWSServer(t, pool)
	full := seedWSKey(t, pool, "alice")

	resp := dialAndExchange(t, url, `{"type":"auth","apiKey":"`+full+`"}`)
	if resp["type"] != "auth_ok" {
		t.Fatalf("got %v, want auth_ok", resp)
	}
	if resp["userID"] != "alice" {
		t.Errorf("userID = %v, want alice", resp["userID"])
	}
}

// expectPolicyClose dials, sends one message, and asserts the server closes the
// connection with a policy violation. Any preceding error/auth_error JSON frame
// (best-effort, racy with the close) is tolerated.
func expectPolicyClose(t *testing.T, url, send string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "done")

	if err := conn.Write(ctx, websocket.MessageText, []byte(send)); err != nil {
		t.Fatalf("write: %v", err)
	}
	for {
		_, _, err := conn.Read(ctx)
		if err == nil {
			continue // an error JSON frame; keep reading for the close
		}
		if websocket.CloseStatus(err) == websocket.StatusPolicyViolation {
			return
		}
		t.Fatalf("expected policy-violation close, got %v", err)
	}
}

func TestWS_AuthInvalidKey(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	url := startWSServer(t, pool)
	expectPolicyClose(t, url, `{"type":"auth","apiKey":"clob_live_bogus"}`)
}

func TestWS_RequiresAuthFirst(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	url := startWSServer(t, pool)
	expectPolicyClose(t, url, `{"type":"subscribe","channel":"depth:BTC-USD"}`)
}
