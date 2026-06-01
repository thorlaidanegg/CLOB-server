package redisstore

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Check increments the per-user per-minute counter and returns true if the request is allowed.
func Check(ctx context.Context, rdb *redis.Client, userID string, limitRPM int) (bool, error) {
	bucket := time.Now().Truncate(time.Minute).Unix()
	key := fmt.Sprintf("ratelimit:%s:%d", userID, bucket)

	pipe := rdb.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 2*time.Minute)
	if _, err := pipe.Exec(ctx); err != nil {
		return true, err // fail open on Redis error
	}
	return incr.Val() <= int64(limitRPM), nil
}
