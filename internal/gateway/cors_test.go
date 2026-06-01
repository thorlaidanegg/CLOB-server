package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newCORSHandler(origins []string) http.Handler {
	mw := corsMiddleware(origins)
	return mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}))
}

func TestCORS_AllowedOrigin(t *testing.T) {
	h := newCORSHandler([]string{"https://app.example.com"})

	req := httptest.NewRequest(http.MethodGet, "/v1/markets", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
		t.Errorf("Allow-Origin = %q, want the request origin", got)
	}
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
}

func TestCORS_DisallowedOrigin(t *testing.T) {
	h := newCORSHandler([]string{"https://app.example.com"})

	req := httptest.NewRequest(http.MethodGet, "/v1/markets", nil)
	req.Header.Set("Origin", "https://evil.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("disallowed origin should get no Allow-Origin header, got %q", got)
	}
	// Request still proceeds (CORS is browser-enforced, server doesn't block).
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (server does not block non-browser clients)", rec.Code)
	}
}

func TestCORS_Wildcard(t *testing.T) {
	h := newCORSHandler([]string{"*"})

	req := httptest.NewRequest(http.MethodGet, "/v1/markets", nil)
	req.Header.Set("Origin", "https://anything.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Errorf("wildcard Allow-Origin = %q, want *", got)
	}
}

func TestCORS_PreflightShortCircuits(t *testing.T) {
	called := false
	mw := corsMiddleware([]string{"*"})
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	}))

	req := httptest.NewRequest(http.MethodOptions, "/v1/orders", nil)
	req.Header.Set("Origin", "https://app.example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if called {
		t.Error("preflight OPTIONS should not reach the wrapped handler")
	}
	if rec.Code != http.StatusNoContent {
		t.Errorf("preflight status = %d, want 204", rec.Code)
	}
}
