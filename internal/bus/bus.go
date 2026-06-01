package bus

import "context"

// Message is a unit of data flowing through the bus.
type Message struct {
	Topic     string
	Key       string
	Value     []byte
	Headers   map[string]string
	Offset    int64
	Partition int32
	SeqNum    uint64 // parsed from headers["seq-num"] by consumer
}

// Producer publishes messages to the bus.
type Producer interface {
	Publish(ctx context.Context, msg Message) error
	Close() error
}

// Consumer reads messages from the bus.
type Consumer interface {
	Subscribe(topic, groupID string) error
	SeekToOffset(partition int32, offset int64) error
	Poll(ctx context.Context) (Message, error)
	Commit(ctx context.Context, msg Message) error
	Close() error
}
