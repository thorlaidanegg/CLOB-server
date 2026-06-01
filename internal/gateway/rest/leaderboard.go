package rest

import (
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
)

type leaderEntry struct {
	UserID string  `json:"userID"`
	Email  string  `json:"email"`
	Score  float64 `json:"score"`
	Rank   int     `json:"rank"`
}

// GetLeaderboard handles GET /v1/leaderboard
func GetLeaderboard(pool *pgxpool.Pool, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entries, err := redisstore.ZRevRange(r.Context(), rdb, "leaderboard", 0, 99)
		if err != nil {
			apierrors.WriteError(w, err)
			return
		}

		result := make([]leaderEntry, 0, len(entries))
		for i, z := range entries {
			userID, _ := z.Member.(string)
			email := ""
			if u, err := pgstore.GetUser(r.Context(), pool, userID); err == nil {
				email = u.Email
			}
			result = append(result, leaderEntry{
				UserID: userID,
				Email:  email,
				Score:  z.Score,
				Rank:   i + 1,
			})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	}
}
