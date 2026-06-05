package auth

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/thorlaidanegg/clob-server/internal/shared/apierrors"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	redisstore "github.com/thorlaidanegg/clob-server/internal/store/redis"
)

// ValidateKey looks up an API key, checking Redis cache then Postgres.
func ValidateKey(ctx context.Context, key string, pg *pgxpool.Pool, rdb *redis.Client) (AuthContext, error) {
	hash := HashKey(key)

	// 1. Redis cache hit
	if cached, ok, _ := redisstore.GetAPIKey(ctx, rdb, hash); ok {
		return AuthContext{
			UserID:    cached.UserID,
			Scopes:    cached.Scopes,
			Tier:      cached.Tier,
			RateLimit: cached.RateLimit,
		}, nil
	}

	// 2. Postgres lookup
	row, err := pgstore.GetAPIKeyByHash(ctx, pg, hash)
	if err != nil {
		return AuthContext{}, apierrors.ErrInvalidKey
	}
	if row.Revoked {
		return AuthContext{}, apierrors.ErrKeyRevoked
	}
	if row.ExpiresAt != nil && row.ExpiresAt.Before(time.Now()) {
		return AuthContext{}, apierrors.ErrKeyExpired
	}

	ac := AuthContext{
		UserID:    row.UserID,
		Scopes:    row.Scopes,
		Tier:      row.Tier,
		RateLimit: row.RateLimit,
	}

	// 3. Cache for 60 seconds
	redisstore.SetAPIKey(ctx, rdb, hash, redisstore.AuthCacheData{
		UserID:    ac.UserID,
		Scopes:    ac.Scopes,
		Tier:      ac.Tier,
		RateLimit: ac.RateLimit,
	}, 60*time.Second)

	// 4. Update last_used_at asynchronously
	go pgstore.UpdateLastUsed(context.Background(), pg, hash)

	return ac, nil
}

// Middleware authenticates each request. Browsers present a JWT session cookie;
// bots present an `Authorization: Bearer <apiKey>`. Either resolves to an
// AuthContext placed on the request context.
func Middleware(pg *pgxpool.Pool, rdb *redis.Client, jwtSecret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// 1. Session cookie (browser).
			if token := ReadSessionToken(r); token != "" {
				if claims, err := ParseSession(jwtSecret, token); err == nil {
					ac := SessionAuthContext(claims)
					next.ServeHTTP(w, r.WithContext(WithContext(r.Context(), ac)))
					return
				}
			}

			// 2. Bearer API key (programmatic clients / bots).
			authHeader := r.Header.Get("Authorization")
			key := strings.TrimPrefix(authHeader, "Bearer ")
			if key == "" || key == authHeader {
				apierrors.WriteError(w, apierrors.ErrUnauthorized)
				return
			}
			ac, err := ValidateKey(r.Context(), key, pg, rdb)
			if err != nil {
				apierrors.WriteError(w, err)
				return
			}
			next.ServeHTTP(w, r.WithContext(WithContext(r.Context(), ac)))
		})
	}
}

// RequireScope returns a middleware that enforces a specific scope.
func RequireScope(scope string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ac, ok := FromContext(r.Context())
			if !ok || !ac.HasScope(scope) {
				apierrors.WriteError(w, apierrors.ErrForbidden)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
