package client

import (
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// circuitBreaker trips after a run of "engine unavailable" failures and stays
// open for a cooldown, during which calls fail fast instead of hammering a dead
// engine. After the cooldown it half-opens to let a single probe through.
type circuitBreaker struct {
	mu        sync.Mutex
	failures  int
	openedAt  time.Time
	threshold int
	cooldown  time.Duration
}

func newCircuitBreaker() *circuitBreaker {
	return &circuitBreaker{threshold: 5, cooldown: 5 * time.Second}
}

// allow reports whether a call may proceed. When the breaker is open and still
// within the cooldown it returns false; once the cooldown elapses it allows a
// single probe (half-open).
func (b *circuitBreaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures < b.threshold {
		return true
	}
	if time.Since(b.openedAt) < b.cooldown {
		return false
	}
	// Half-open: let one probe through. A success resets; a failure re-opens.
	b.failures = b.threshold - 1
	return true
}

// record updates breaker state from a call's result. Only "Unavailable" (engine
// down/unreachable) counts toward tripping; application errors do not.
func (b *circuitBreaker) record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.failures = 0
		return
	}
	if status.Code(err) == codes.Unavailable {
		b.failures++
		if b.failures == b.threshold {
			b.openedAt = time.Now()
		}
	}
}
