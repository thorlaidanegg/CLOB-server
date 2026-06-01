package auth

import (
	"context"
	"strings"
	"testing"
)

func TestGenerateKey_FormatAndHash(t *testing.T) {
	full, hash, prefix, err := GenerateKey("clob_live")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(full, "clob_live_") {
		t.Errorf("full key %q missing prefix", full)
	}
	if hash != HashKey(full) {
		t.Errorf("returned hash must equal HashKey(full)")
	}
	if len(hash) != 64 {
		t.Errorf("sha256 hex hash should be 64 chars, got %d", len(hash))
	}
	if !strings.HasPrefix(prefix, "clob_live_") {
		t.Errorf("display prefix %q malformed", prefix)
	}
	// Prefix must not leak the full key.
	if prefix == full {
		t.Error("display prefix must not equal the full key")
	}
}

func TestGenerateKey_Unique(t *testing.T) {
	seen := make(map[string]bool)
	for i := 0; i < 500; i++ {
		full, _, _, err := GenerateKey("clob_live")
		if err != nil {
			t.Fatal(err)
		}
		if seen[full] {
			t.Fatalf("duplicate key generated: %q", full)
		}
		seen[full] = true
	}
}

func TestHashKey_Deterministic(t *testing.T) {
	a := HashKey("clob_live_abc")
	b := HashKey("clob_live_abc")
	if a != b {
		t.Error("HashKey must be deterministic")
	}
	if HashKey("clob_live_abc") == HashKey("clob_live_abd") {
		t.Error("different keys must hash differently")
	}
}

func TestAuthContext_HasScope(t *testing.T) {
	cases := []struct {
		name   string
		scopes []string
		check  string
		want   bool
	}{
		{"exact match", []string{"trade:write"}, "trade:write", true},
		{"missing", []string{"trade:read"}, "trade:write", false},
		{"admin grants all", []string{"admin:all"}, "trade:write", true},
		{"admin grants admin", []string{"admin:all"}, "admin:all", true},
		{"empty scopes", nil, "trade:read", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ac := AuthContext{Scopes: tc.scopes}
			if got := ac.HasScope(tc.check); got != tc.want {
				t.Errorf("HasScope(%q) = %v, want %v", tc.check, got, tc.want)
			}
		})
	}
}

func TestContext_RoundTrip(t *testing.T) {
	ac := AuthContext{UserID: "usr_42", Scopes: []string{"trade:read"}, Tier: "standard", RateLimit: 300}
	ctx := WithContext(context.Background(), ac)

	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("FromContext returned ok=false after WithContext")
	}
	if got.UserID != "usr_42" || got.Tier != "standard" || got.RateLimit != 300 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
}

func TestFromContext_Absent(t *testing.T) {
	_, ok := FromContext(context.Background())
	if ok {
		t.Error("FromContext on bare context should return ok=false")
	}
}
