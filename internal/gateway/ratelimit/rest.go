package ratelimit

import (
	"net/http"

	"github.com/redis/go-redis/v9"
	"github.com/thorlaidanegg/clob-server/internal/gateway/auth"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
)

// Middleware enforces per-user per-minute rate limits via Redis.
func Middleware(rdb *redis.Client, limitRPM int) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ac, ok := auth.FromContext(r.Context())
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			limit := limitRPM
			if ac.RateLimit > 0 {
				limit = ac.RateLimit
			}

			allowed, err := redisstore.Check(r.Context(), rdb, ac.UserID, limit)
			if err != nil || !allowed {
				apierrors.WriteErrorMsg(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
