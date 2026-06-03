package ws

import (
	"testing"
	"time"
)

func waitConn(h *Hub, id string, want bool) bool {
	for i := 0; i < 200; i++ {
		h.mu.RLock()
		_, ok := h.connections[id]
		h.mu.RUnlock()
		if ok == want {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}

func newTestClient(h *Hub, id string, bufSize int) *Client {
	return &Client{id: id, send: make(chan []byte, bufSize), hub: h}
}

func recvWithin(t *testing.T, c *Client, d time.Duration) ([]byte, bool) {
	t.Helper()
	select {
	case b := <-c.send:
		return b, true
	case <-time.After(d):
		return nil, false
	}
}

func TestHub_BroadcastFansOutToSubscribers(t *testing.T) {
	h := NewHub()
	go h.Run()

	c1 := newTestClient(h, "c1", 4)
	c2 := newTestClient(h, "c2", 4)
	c3 := newTestClient(h, "c3", 4) // not subscribed
	for _, c := range []*Client{c1, c2, c3} {
		h.register <- c
		if !waitConn(h, c.id, true) {
			t.Fatalf("client %s never registered", c.id)
		}
	}

	h.Subscribe("c1", "depth:BTC-USD")
	h.Subscribe("c2", "depth:BTC-USD")
	h.Broadcast("depth:BTC-USD", []byte(`{"x":1}`))

	if _, ok := recvWithin(t, c1, time.Second); !ok {
		t.Error("c1 (subscribed) did not receive broadcast")
	}
	if _, ok := recvWithin(t, c2, time.Second); !ok {
		t.Error("c2 (subscribed) did not receive broadcast")
	}
	if _, ok := recvWithin(t, c3, 100*time.Millisecond); ok {
		t.Error("c3 (not subscribed) should not receive broadcast")
	}
}

func TestHub_UnsubscribeStopsDelivery(t *testing.T) {
	h := NewHub()
	go h.Run()

	c := newTestClient(h, "c1", 4)
	h.register <- c
	waitConn(h, "c1", true)

	h.Subscribe("c1", "trades:BTC-USD")
	h.Unsubscribe("c1", "trades:BTC-USD")
	h.Broadcast("trades:BTC-USD", []byte(`{"t":1}`))

	if _, ok := recvWithin(t, c, 100*time.Millisecond); ok {
		t.Error("unsubscribed client should not receive broadcasts")
	}
}

func TestHub_SlowConsumerIsEvicted(t *testing.T) {
	h := NewHub()
	go h.Run()

	// send buffer of 1, pre-filled so the next broadcast cannot enqueue.
	c := newTestClient(h, "slow", 1)
	c.send <- []byte("backlog")
	h.register <- c
	waitConn(h, "slow", true)

	h.Subscribe("slow", "depth:BTC-USD")
	h.Broadcast("depth:BTC-USD", []byte(`{"x":1}`))

	if !waitConn(h, "slow", false) {
		t.Error("slow consumer with a full send buffer should be evicted")
	}
}
