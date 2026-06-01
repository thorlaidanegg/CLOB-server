package ws

import "sync"

// BroadcastMsg carries a channel name and JSON payload to fan out.
type BroadcastMsg struct {
	Channel string
	Payload []byte
}

// Hub manages WebSocket connections and channel subscriptions.
type Hub struct {
	connections map[string]*Client
	channels    map[string]map[string]*Client // channel → connID → client
	mu          sync.RWMutex
	register    chan *Client
	unregister  chan *Client
	broadcast   chan BroadcastMsg
}

// NewHub creates a Hub.
func NewHub() *Hub {
	return &Hub{
		connections: make(map[string]*Client),
		channels:    make(map[string]map[string]*Client),
		register:    make(chan *Client, 64),
		unregister:  make(chan *Client, 64),
		broadcast:   make(chan BroadcastMsg, 4096),
	}
}

// Run is the hub's main loop. Must be called in a goroutine.
func (h *Hub) Run() {
	for {
		select {
		case c := <-h.register:
			h.mu.Lock()
			h.connections[c.id] = c
			h.mu.Unlock()

		case c := <-h.unregister:
			h.mu.Lock()
			delete(h.connections, c.id)
			for _, subs := range h.channels {
				delete(subs, c.id)
			}
			h.mu.Unlock()
			close(c.send)

		case msg := <-h.broadcast:
			h.mu.RLock()
			subs := h.channels[msg.Channel]
			h.mu.RUnlock()
			for _, c := range subs {
				select {
				case c.send <- msg.Payload:
				default:
					// slow consumer — disconnect
					h.unregister <- c
				}
			}
		}
	}
}

// Subscribe adds a client to a channel.
func (h *Hub) Subscribe(connID, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.channels[channel] == nil {
		h.channels[channel] = make(map[string]*Client)
	}
	if c, ok := h.connections[connID]; ok {
		h.channels[channel][connID] = c
	}
}

// Unsubscribe removes a client from a channel.
func (h *Hub) Unsubscribe(connID, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.channels[channel], connID)
}

// Broadcast sends a message to all subscribers of a channel.
func (h *Hub) Broadcast(channel string, payload []byte) {
	select {
	case h.broadcast <- BroadcastMsg{Channel: channel, Payload: payload}:
	default:
	}
}
