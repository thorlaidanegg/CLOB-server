package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// AuthCacheData is what we cache for an API key lookup.
type AuthCacheData struct {
	UserID    string   `json:"userID"`
	Scopes    []string `json:"scopes"`
	Tier      string   `json:"tier"`
	RateLimit int      `json:"rateLimit"`
}

func apiKeyRedisKey(hash string) string { return "apikey:" + hash }
func bboRedisKey(marketID string) string { return "bbo:" + marketID }
func lastPriceRedisKey(marketID string) string { return "lastprice:" + marketID }

// SetAPIKey stores auth data for an API key hash.
func SetAPIKey(ctx context.Context, rdb *redis.Client, hash string, data AuthCacheData, ttl time.Duration) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return rdb.Set(ctx, apiKeyRedisKey(hash), b, ttl).Err()
}

// GetAPIKey retrieves cached auth data for a key hash.
func GetAPIKey(ctx context.Context, rdb *redis.Client, hash string) (AuthCacheData, bool, error) {
	b, err := rdb.Get(ctx, apiKeyRedisKey(hash)).Bytes()
	if errors.Is(err, redis.Nil) {
		return AuthCacheData{}, false, nil
	}
	if err != nil {
		return AuthCacheData{}, false, err
	}
	var data AuthCacheData
	if err := json.Unmarshal(b, &data); err != nil {
		return AuthCacheData{}, false, err
	}
	return data, true, nil
}

// SetBBO caches the best bid and ask for a market (TTL 5s).
func SetBBO(ctx context.Context, rdb *redis.Client, marketID, bid, ask string) error {
	return rdb.HSet(ctx, bboRedisKey(marketID), "bid", bid, "ask", ask).Err()
}

// GetBBO returns the cached BBO. ok=false if no BBO is cached.
func GetBBO(ctx context.Context, rdb *redis.Client, marketID string) (bid, ask string, ok bool, err error) {
	vals, err := rdb.HMGet(ctx, bboRedisKey(marketID), "bid", "ask").Result()
	if err != nil {
		return "", "", false, err
	}
	if vals[0] == nil || vals[1] == nil {
		return "", "", false, nil
	}
	bid, _ = vals[0].(string)
	ask, _ = vals[1].(string)
	return bid, ask, bid != "" || ask != "", nil
}

// SetLastPrice stores the last traded price for a market.
func SetLastPrice(ctx context.Context, rdb *redis.Client, marketID, price string) error {
	return rdb.Set(ctx, lastPriceRedisKey(marketID), price, 0).Err()
}

// GetLastPrice returns the last traded price. ok=false if not set.
func GetLastPrice(ctx context.Context, rdb *redis.Client, marketID string) (string, bool, error) {
	price, err := rdb.Get(ctx, lastPriceRedisKey(marketID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	return price, err == nil, err
}
