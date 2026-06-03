package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/jackc/pgx/v5/pgxpool"
)

// seedKey inserts a user + api key and returns the full bearer token.
func seedKey(t *testing.T, pool *pgxpool.Pool, userID string, scopes []string, tier string, expires *time.Time) string {
	t.Helper()
	ctx := context.Background()
	if err := pgstore.InsertUser(ctx, pool, userID, userID+"@test.local"); err != nil {
		t.Fatal(err)
	}
	full, hash, prefix, err := auth.GenerateKey("clob_live")
	if err != nil {
		t.Fatal(err)
	}
	if err := pgstore.InsertAPIKey(ctx, pool, pgstore.APIKeyRow{
		UserID: userID, KeyHash: hash, KeyPrefix: prefix, Name: "test",
		Scopes: scopes, Tier: tier, RateLimit: 300, ExpiresAt: expires,
	}); err != nil {
		t.Fatal(err)
	}
	return full
}

func TestValidateKey_CacheHitAfterDBDelete(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	rdb := testsupport.RequireMiniRedis(t)
	ctx := context.Background()

	full := seedKey(t, pool, "alice", []string{"trade:write"}, "standard", nil)

	// Cold path: Postgres lookup populates the Redis cache.
	ac, err := auth.ValidateKey(ctx, full, pool, rdb)
	if err != nil || ac.UserID != "alice" {
		t.Fatalf("cold validate: ac=%+v err=%v", ac, err)
	}

	// Remove the DB row; a cache hit must still authenticate within the TTL.
	if _, err := pool.Exec(ctx, `DELETE FROM api_keys WHERE key_hash=$1`, auth.HashKey(full)); err != nil {
		t.Fatal(err)
	}
	ac2, err := auth.ValidateKey(ctx, full, pool, rdb)
	if err != nil || ac2.UserID != "alice" {
		t.Fatalf("expected cache hit, got ac=%+v err=%v", ac2, err)
	}
}

func TestValidateKey_RevokedAndExpired(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	rdb := testsupport.RequireMiniRedis(t)
	ctx := context.Background()

	revoked := seedKey(t, pool, "bob", []string{"trade:read"}, "standard", nil)
	if _, err := pool.Exec(ctx, `UPDATE api_keys SET revoked=true WHERE key_hash=$1`, auth.HashKey(revoked)); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.ValidateKey(ctx, revoked, pool, rdb); !errors.Is(err, apierrors.ErrKeyRevoked) {
		t.Errorf("revoked key: got %v, want ErrKeyRevoked", err)
	}

	past := time.Now().Add(-time.Hour)
	expired := seedKey(t, pool, "carol", []string{"trade:read"}, "standard", &past)
	if _, err := auth.ValidateKey(ctx, expired, pool, rdb); !errors.Is(err, apierrors.ErrKeyExpired) {
		t.Errorf("expired key: got %v, want ErrKeyExpired", err)
	}

	if _, err := auth.ValidateKey(ctx, "clob_live_doesnotexist", pool, rdb); !errors.Is(err, apierrors.ErrInvalidKey) {
		t.Errorf("unknown key: got %v, want ErrInvalidKey", err)
	}
}

func TestMiddleware_HeaderHandling(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	rdb := testsupport.RequireMiniRedis(t)

	full := seedKey(t, pool, "dave", []string{"trade:write"}, "standard", nil)

	var sawUser string
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ac, ok := auth.FromContext(r.Context()); ok {
			sawUser = ac.UserID
		}
		w.WriteHeader(http.StatusOK)
	})
	handler := auth.Middleware(pool, rdb)(next)

	cases := []struct {
		name     string
		header   string
		wantCode int
		wantUser string
	}{
		{"valid bearer", "Bearer " + full, http.StatusOK, "dave"},
		{"missing header", "", http.StatusUnauthorized, ""},
		{"no bearer prefix", full, http.StatusUnauthorized, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sawUser = ""
			req := httptest.NewRequest(http.MethodGet, "/v1/orders", nil)
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantCode)
			}
			if sawUser != tc.wantUser {
				t.Errorf("handler saw user %q, want %q", sawUser, tc.wantUser)
			}
		})
	}
}

func TestRequireScope_Gating(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	guarded := auth.RequireScope("admin:all")(next)

	cases := []struct {
		name     string
		scopes   []string
		wantCode int
	}{
		{"has admin", []string{"admin:all"}, http.StatusOK},
		{"missing admin", []string{"trade:write"}, http.StatusForbidden},
		{"no auth context", nil, http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/admin/markets", nil)
			if tc.scopes != nil {
				ctx := auth.WithContext(req.Context(), auth.AuthContext{UserID: "x", Scopes: tc.scopes})
				req = req.WithContext(ctx)
			}
			rec := httptest.NewRecorder()
			guarded.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantCode)
			}
		})
	}
}
