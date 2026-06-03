package admin_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/thorlaidanegg/clob-server/internal/gateway/admin"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
)

func TestCreditUser_GrantsCredits(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	ctx := context.Background()
	walletStore := wallet.NewPgStore(pool, 2)

	h := admin.CreditUser(pool, walletStore)

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/users/alice/credits",
		strings.NewReader(`{"amount":"1000.00"}`))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "alice")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", rec.Code, rec.Body.String())
	}

	var available int64
	if err := pool.QueryRow(ctx, `SELECT available FROM wallets WHERE user_id='alice'`).Scan(&available); err != nil {
		t.Fatal(err)
	}
	if available != 100000 { // 1000.00 at precision 2
		t.Errorf("available = %d, want 100000", available)
	}
}

func TestCreditUser_BadAmount(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	h := admin.CreditUser(pool, wallet.NewPgStore(pool, 2))

	req := httptest.NewRequest(http.MethodPost, "/v1/admin/users/alice/credits",
		strings.NewReader(`{"amount":"not-a-number"}`))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("id", "alice")
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}
