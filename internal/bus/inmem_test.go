package bus

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestInMemBus_FanOutToAllConsumers(t *testing.T) {
	b := NewInMemBus()
	defer b.Close()

	c1 := b.NewConsumer()
	c2 := b.NewConsumer()

	msg := Message{Topic: "market-events", Key: "BTC-USD", Value: []byte("hello")}
	if err := b.Publish(context.Background(), msg); err != nil {
		t.Fatalf("publish: %v", err)
	}

	for i, c := range []Consumer{c1, c2} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, err := c.Poll(ctx)
		cancel()
		if err != nil {
			t.Fatalf("consumer %d poll: %v", i, err)
		}
		if string(got.Value) != "hello" || got.Key != "BTC-USD" {
			t.Errorf("consumer %d got %+v", i, got)
		}
	}
}

func TestInMemBus_IndependentConsumers(t *testing.T) {
	// Each consumer has its own buffer; draining one must not affect the other.
	b := NewInMemBus()
	defer b.Close()

	c1 := b.NewConsumer()
	c2 := b.NewConsumer()

	for i := 0; i < 3; i++ {
		b.Publish(context.Background(), Message{Value: []byte{byte(i)}})
	}

	// Drain c1 fully.
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, err := c1.Poll(ctx)
		cancel()
		if err != nil {
			t.Fatalf("c1 poll %d: %v", i, err)
		}
		if got.Value[0] != byte(i) {
			t.Errorf("c1 message %d out of order: %v", i, got.Value)
		}
	}

	// c2 should still have all 3 buffered.
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		got, err := c2.Poll(ctx)
		cancel()
		if err != nil {
			t.Fatalf("c2 poll %d: %v", i, err)
		}
		if got.Value[0] != byte(i) {
			t.Errorf("c2 message %d out of order: %v", i, got.Value)
		}
	}
}

func TestInMemBus_PollRespectsContextCancel(t *testing.T) {
	b := NewInMemBus()
	defer b.Close()
	c := b.NewConsumer()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := c.Poll(ctx)
	if err == nil {
		t.Error("Poll should return error when context expires with no message")
	}
}

func TestInMemBus_CloseUnblocksPoll(t *testing.T) {
	b := NewInMemBus()
	c := b.NewConsumer()

	done := make(chan error, 1)
	go func() {
		_, err := c.Poll(context.Background())
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	b.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Error("Poll should return an error after bus is closed")
		}
	case <-time.After(time.Second):
		t.Fatal("Poll did not unblock after Close")
	}
}

func TestInMemBus_PublishNonBlockingWhenBufferFull(t *testing.T) {
	// A slow consumer must not block the publisher. We publish far more than the
	// buffer (1024) without anyone draining; Publish must return promptly.
	b := NewInMemBus()
	defer b.Close()
	_ = b.NewConsumer() // never drained

	done := make(chan struct{})
	go func() {
		for i := 0; i < 5000; i++ {
			b.Publish(context.Background(), Message{Value: []byte("x")})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish blocked on a full consumer buffer")
	}
}

func TestInMemBus_ConcurrentPublish(t *testing.T) {
	b := NewInMemBus()
	defer b.Close()
	_ = b.NewConsumer()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b.Publish(context.Background(), Message{Value: []byte("y")})
			}
		}()
	}
	wg.Wait() // race detector will flag unsynchronized access
}
