package ratelimit

import "time"

// WSLimiter enforces per-connection in-memory rate limits (no Redis needed).
type WSLimiter struct {
	count    int
	resetAt  time.Time
	limitRPS int
}

// NewWSLimiter creates a per-connection limiter with the given requests-per-second limit.
func NewWSLimiter(limitRPS int) *WSLimiter {
	return &WSLimiter{limitRPS: limitRPS, resetAt: time.Now().Add(time.Second)}
}

// Allow returns true if the request is within the rate limit.
func (l *WSLimiter) Allow() bool {
	now := time.Now()
	if now.After(l.resetAt) {
		l.count = 0
		l.resetAt = now.Add(time.Second)
	}
	l.count++
	return l.count <= l.limitRPS
}
