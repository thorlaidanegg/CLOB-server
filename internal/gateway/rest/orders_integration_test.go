package rest_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/gateway/rest"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	ordersstore "github.com/thorlaidanegg/clob-server/internal/store/postgres/orders"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/jackc/pgx/v5/pgxpool"
)

func seedMarket(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	err := pgstore.InsertMarket(context.Background(), pool, pgstore.MarketRow{
		MarketID: "BTC-USD", BaseAsset: "BTC", QuoteAsset: "USD",
		PricePrecision: 2, QtyPrecision: 0, TickSize: 1, LotSize: 1,
		Features: 1, FeeModel: "flat", State: "open",
	})
	if err != nil {
		t.Fatal(err)
	}
}

func authReq(method, target, body string, userID string, params map[string]string) *http.Request {
	var r *http.Request
	if body != "" {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	ctx := auth.WithContext(r.Context(), auth.AuthContext{UserID: userID, Scopes: []string{"trade:write", "trade:read"}})
	if len(params) > 0 {
		rctx := chi.NewRouteContext()
		for k, v := range params {
			rctx.URLParams.Add(k, v)
		}
		ctx = context.WithValue(ctx, chi.RouteCtxKey, rctx)
	}
	return r.WithContext(ctx)
}

func TestPlaceOrder_HappyPath(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	seedMarket(t, pool)
	store := ordersstore.NewPgStore(pool)
	fake := &testsupport.FakeEngine{}

	h := rest.PlaceOrder(pool, store, fake)
	body := `{"marketID":"BTC-USD","side":"bid","orderType":"limit","price":"100.00","qty":"5","tif":"GTC"}`
	req := authReq(http.MethodPost, "/v1/orders", body, "alice", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if fake.PlacedCount() != 1 {
		t.Errorf("engine PlaceOrder calls = %d, want 1", fake.PlacedCount())
	}
	// The order must be persisted before the engine call.
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM orders WHERE user_id='alice' AND market_id='BTC-USD'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("persisted orders = %d, want 1", n)
	}
}

func TestPlaceOrder_BadRequests(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	seedMarket(t, pool)
	h := rest.PlaceOrder(pool, ordersstore.NewPgStore(pool), &testsupport.FakeEngine{})

	cases := []struct {
		name, body string
		want       int
	}{
		{"malformed json", `{not json`, http.StatusBadRequest},
		{"unknown market", `{"marketID":"NOPE","side":"bid","orderType":"limit","price":"1.00","qty":"1","tif":"GTC"}`, http.StatusNotFound},
		{"invalid side", `{"marketID":"BTC-USD","side":"sideways","orderType":"limit","price":"1.00","qty":"1"}`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := authReq(http.MethodPost, "/v1/orders", tc.body, "alice", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Errorf("status = %d, want %d; body=%s", rec.Code, tc.want, rec.Body.String())
			}
		})
	}
}

func TestGetAndCancelOrder_Ownership(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	seedMarket(t, pool)
	store := ordersstore.NewPgStore(pool)
	ctx := context.Background()

	if err := store.InsertOrder(ctx, ordersstore.OrderRow{
		OrderID: "ord_bob", UserID: "bob", MarketID: "BTC-USD", Side: "bid",
		OrderType: "limit", Price: 10000, OrigQty: 5, RemainQty: 5, Status: "rested", TIF: "GTC",
	}); err != nil {
		t.Fatal(err)
	}

	// GET as the owner → 200; as a stranger → 403.
	getH := rest.GetOrder(store)
	rec := httptest.NewRecorder()
	getH.ServeHTTP(rec, authReq(http.MethodGet, "/v1/orders/ord_bob", "", "bob", map[string]string{"id": "ord_bob"}))
	if rec.Code != http.StatusOK {
		t.Errorf("owner GET status = %d, want 200", rec.Code)
	}
	rec = httptest.NewRecorder()
	getH.ServeHTTP(rec, authReq(http.MethodGet, "/v1/orders/ord_bob", "", "alice", map[string]string{"id": "ord_bob"}))
	if rec.Code != http.StatusForbidden {
		t.Errorf("stranger GET status = %d, want 403", rec.Code)
	}

	// CancelOrder by a stranger must not reach the engine.
	fake := &testsupport.FakeEngine{}
	cancelH := rest.CancelOrder(pool, store, fake)
	rec = httptest.NewRecorder()
	cancelH.ServeHTTP(rec, authReq(http.MethodDelete, "/v1/orders/ord_bob", "", "alice", map[string]string{"id": "ord_bob"}))
	if rec.Code != http.StatusForbidden {
		t.Errorf("stranger cancel status = %d, want 403", rec.Code)
	}
	if fake.CanceledCount() != 0 {
		t.Errorf("engine cancel calls = %d, want 0 (stranger blocked)", fake.CanceledCount())
	}

	// Owner cancel reaches the engine.
	rec = httptest.NewRecorder()
	cancelH.ServeHTTP(rec, authReq(http.MethodDelete, "/v1/orders/ord_bob", "", "bob", map[string]string{"id": "ord_bob"}))
	if rec.Code != http.StatusAccepted {
		t.Errorf("owner cancel status = %d, want 202", rec.Code)
	}
	if fake.CanceledCount() != 1 {
		t.Errorf("engine cancel calls = %d, want 1", fake.CanceledCount())
	}
}
