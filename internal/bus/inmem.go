package bus

import (
	"context"
	"fmt"
	"sync"
)

// InMemBus is a fan-out message bus for ROLE=all (no Kafka).
// Each NewConsumer() call returns an independent subscriber; all receive every message.
type InMemBus struct {
	mu          sync.RWMutex
	subscribers []chan Message
	closed      bool
}

// NewInMemBus creates an empty InMemBus.
func NewInMemBus() *InMemBus {
	return &InMemBus{}
}

// Publish delivers a message to all subscriber channels.
// Non-blocking per subscriber: drops messages to full channels and continues.
func (b *InMemBus) Publish(_ context.Context, msg Message) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, ch := range b.subscribers {
		select {
		case ch <- msg:
		default:
			// subscriber is slow — drop message for this subscriber only
		}
	}
	return nil
}

// NewConsumer returns an independent Consumer that receives all published messages.
func (b *InMemBus) NewConsumer() Consumer {
	ch := make(chan Message, 1024)
	b.mu.Lock()
	b.subscribers = append(b.subscribers, ch)
	b.mu.Unlock()
	return &inMemConsumer{ch: ch}
}

// Close closes all subscriber channels.
func (b *InMemBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	for _, ch := range b.subscribers {
		close(ch)
	}
	return nil
}

// inMemConsumer is a single fan-out subscriber.
type inMemConsumer struct {
	ch chan Message
}

func (c *inMemConsumer) Subscribe(_, _ string) error  { return nil }
func (c *inMemConsumer) SeekToOffset(int32, int64) error { return nil }
func (c *inMemConsumer) Commit(_ context.Context, _ Message) error { return nil }
func (c *inMemConsumer) Close() error { return nil }

func (c *inMemConsumer) Poll(ctx context.Context) (Message, error) {
	select {
	case msg, ok := <-c.ch:
		if !ok {
			return Message{}, fmt.Errorf("inmem bus closed")
		}
		return msg, nil
	case <-ctx.Done():
		return Message{}, ctx.Err()
	}
}
