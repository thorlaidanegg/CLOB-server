package ratelimit

import (
	"testing"
	"time"
)

func TestWSLimiter_AllowsUpToLimit(t *testing.T) {
	l := NewWSLimiter(5)
	for i := 0; i < 5; i++ {
		if !l.Allow() {
			t.Fatalf("request %d should be allowed within limit of 5", i+1)
		}
	}
	if l.Allow() {
		t.Error("6th request should be denied")
	}
}

func TestWSLimiter_ResetsAfterWindow(t *testing.T) {
	l := NewWSLimiter(2)
	if !l.Allow() || !l.Allow() {
		t.Fatal("first two should be allowed")
	}
	if l.Allow() {
		t.Fatal("third should be denied before reset")
	}

	// Force the window to expire.
	l.resetAt = time.Now().Add(-time.Second)
	if !l.Allow() {
		t.Error("after window reset the limiter should allow again")
	}
}
