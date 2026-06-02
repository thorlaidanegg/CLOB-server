package client

import (
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func unavailable() error { return status.Error(codes.Unavailable, "engine down") }

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	b := newCircuitBreaker()

	// Below threshold: still allowed.
	for i := 0; i < b.threshold-1; i++ {
		if !b.allow() {
			t.Fatalf("should allow before threshold (i=%d)", i)
		}
		b.record(unavailable())
	}
	if !b.allow() {
		t.Fatal("the threshold-th call should still be allowed (it's the one that trips it)")
	}
	b.record(unavailable()) // this hits the threshold

	if b.allow() {
		t.Error("breaker should be open after threshold consecutive Unavailable errors")
	}
}

func TestCircuitBreaker_SuccessResets(t *testing.T) {
	b := newCircuitBreaker()
	for i := 0; i < b.threshold; i++ {
		b.allow()
		b.record(unavailable())
	}
	if b.allow() {
		t.Fatal("should be open")
	}
	// Force cooldown to elapse → half-open probe allowed → success resets.
	b.openedAt = time.Now().Add(-2 * b.cooldown)
	if !b.allow() {
		t.Fatal("should allow a probe after cooldown")
	}
	b.record(nil) // probe succeeds
	if !b.allow() {
		t.Error("a successful probe should fully close the breaker")
	}
}

func TestCircuitBreaker_NonUnavailableDoesNotTrip(t *testing.T) {
	b := newCircuitBreaker()
	for i := 0; i < b.threshold+2; i++ {
		b.allow()
		b.record(errors.New("invalid argument")) // application error, not Unavailable
	}
	if !b.allow() {
		t.Error("application errors must not trip the breaker — only Unavailable")
	}
}

func TestCircuitBreaker_HalfOpenReopensOnFailure(t *testing.T) {
	b := newCircuitBreaker()
	for i := 0; i < b.threshold; i++ {
		b.allow()
		b.record(unavailable())
	}
	b.openedAt = time.Now().Add(-2 * b.cooldown)

	if !b.allow() {
		t.Fatal("probe should be allowed after cooldown")
	}
	b.record(unavailable()) // probe fails → re-open
	if b.allow() {
		t.Error("a failed probe should re-open the breaker")
	}
}
