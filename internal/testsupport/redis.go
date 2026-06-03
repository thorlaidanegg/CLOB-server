package testsupport

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// RequireMiniRedis starts an in-process Redis (miniredis) and returns a connected
// client. No external Redis or container is needed, so these tests run anywhere,
// including CI. The server is shut down on test cleanup.
func RequireMiniRedis(t *testing.T) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("testsupport: start miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		rdb.Close()
		mr.Close()
	})
	return rdb
}
