package redisstore

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// ZAdd adds or updates a member's score in a sorted set.
func ZAdd(ctx context.Context, rdb *redis.Client, key, member string, score float64) error {
	return rdb.ZAdd(ctx, key, redis.Z{Score: score, Member: member}).Err()
}

// ZRevRange returns members in descending score order.
func ZRevRange(ctx context.Context, rdb *redis.Client, key string, start, stop int64) ([]redis.Z, error) {
	return rdb.ZRevRangeWithScores(ctx, key, start, stop).Result()
}

// ZIncrBy increments a member's score by increment atomically.
func ZIncrBy(ctx context.Context, rdb *redis.Client, key string, increment float64, member string) error {
	return rdb.ZIncrBy(ctx, key, increment, member).Err()
}
