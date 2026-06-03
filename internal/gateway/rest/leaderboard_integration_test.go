package rest_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/thorlaidanegg/clob-server/internal/gateway/rest"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	"github.com/thorlaidanegg/clob-server/internal/testsupport"
)

func TestGetLeaderboard_RanksWithEmails(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	rdb := testsupport.RequireMiniRedis(t)
	ctx := context.Background()

	if err := pgstore.InsertUser(ctx, pool, "alice", "alice@test.local"); err != nil {
		t.Fatal(err)
	}
	if err := pgstore.InsertUser(ctx, pool, "bob", "bob@test.local"); err != nil {
		t.Fatal(err)
	}
	rdb.ZAdd(ctx, "leaderboard", redis.Z{Score: 250, Member: "alice"})
	rdb.ZAdd(ctx, "leaderboard", redis.Z{Score: 100, Member: "bob"})

	h := rest.GetLeaderboard(pool, rdb)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/leaderboard", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var got []struct {
		UserID string  `json:"userID"`
		Email  string  `json:"email"`
		Score  float64 `json:"score"`
		Rank   int     `json:"rank"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("entries = %d, want 2", len(got))
	}
	// Highest score first.
	if got[0].UserID != "alice" || got[0].Rank != 1 || got[0].Email != "alice@test.local" || got[0].Score != 250 {
		t.Errorf("rank 1 = %+v, want alice/250/email", got[0])
	}
	if got[1].UserID != "bob" || got[1].Rank != 2 {
		t.Errorf("rank 2 = %+v, want bob", got[1])
	}
}
