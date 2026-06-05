package bus

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// KafkaProducer implements Producer using franz-go.
type KafkaProducer struct {
	client *kgo.Client
}

// NewKafkaProducer creates a Kafka producer connecting to the given brokers.
func NewKafkaProducer(brokers []string) (*KafkaProducer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Let the first produce create the topic if an operator hasn't pre-created
		// it (the broker must also permit auto-create). Without this a fresh
		// cluster rejects every event with UNKNOWN_TOPIC_OR_PARTITION.
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &KafkaProducer{client: client}, nil
}

func (p *KafkaProducer) Publish(ctx context.Context, msg Message) error {
	headers := make([]kgo.RecordHeader, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	rec := &kgo.Record{
		Topic:   msg.Topic,
		Key:     []byte(msg.Key),
		Value:   msg.Value,
		Headers: headers,
	}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

func (p *KafkaProducer) Close() error {
	p.client.Close()
	return nil
}

// KafkaConsumer implements Consumer using franz-go with manual commits.
//
// A single PollFetches returns a *batch* of records. Poll hands them back one at
// a time from buf so callers (which process one Message per call) never lose the
// rest of a batch — critical because a single trade emits a burst of events
// (accepts, fills, trade_executed, depth) that arrive in one fetch.
type KafkaConsumer struct {
	client *kgo.Client
	buf    []Message
}

// NewKafkaConsumer creates a Kafka consumer in the given group.
func NewKafkaConsumer(brokers []string, groupID string) (*KafkaConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.DisableAutoCommit(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &KafkaConsumer{client: client}, nil
}

// NewKafkaRecoveryConsumer creates a one-shot consumer that reads a topic from
// the beginning, used to fold the event log during crash recovery. It uses a
// unique throwaway group so each recovery run replays the full retained log;
// the bookstate fold is idempotent (events at or below the checkpoint seq are
// skipped), so re-reading already-checkpointed events is cheap and safe.
func NewKafkaRecoveryConsumer(brokers []string) (*KafkaConsumer, error) {
	group := fmt.Sprintf("engine-recovery-%d", time.Now().UnixNano())
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka recovery consumer: %w", err)
	}
	return &KafkaConsumer{client: client}, nil
}

func (c *KafkaConsumer) Subscribe(topic, _ string) error {
	c.client.AddConsumeTopics(topic)
	return nil
}

func (c *KafkaConsumer) SeekToOffset(partition int32, offset int64) error {
	// franz-go: seek is per-partition; use ConsumePartitions for fine-grained control.
	// For simplicity, we rely on seqNum idempotency for correctness; seek is best-effort.
	_ = partition
	_ = offset
	return nil
}

func (c *KafkaConsumer) Poll(ctx context.Context) (Message, error) {
	// Drain the buffered batch first so no record in a fetch is ever dropped.
	if len(c.buf) > 0 {
		msg := c.buf[0]
		c.buf = c.buf[1:]
		return msg, nil
	}

	fetches := c.client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return Message{}, fmt.Errorf("kafka consumer closed")
	}
	if err := fetches.Err(); err != nil {
		return Message{}, fmt.Errorf("kafka poll: %w", err)
	}

	fetches.EachRecord(func(r *kgo.Record) {
		headers := make(map[string]string, len(r.Headers))
		for _, h := range r.Headers {
			headers[h.Key] = string(h.Value)
		}
		var seqNum uint64
		if seq, err := strconv.ParseUint(headers["seq-num"], 10, 64); err == nil {
			seqNum = seq
		}
		c.buf = append(c.buf, Message{
			Topic:     r.Topic,
			Key:       string(r.Key),
			Value:     r.Value,
			Headers:   headers,
			Offset:    r.Offset,
			Partition: r.Partition,
			SeqNum:    seqNum,
		})
	})

	if len(c.buf) == 0 {
		// Empty fetch (poll timeout / ctx). Return an empty message; the caller's
		// deserialize step treats it as an unknown event and moves on.
		return Message{}, nil
	}
	msg := c.buf[0]
	c.buf = c.buf[1:]
	return msg, nil
}

func (c *KafkaConsumer) Commit(ctx context.Context, msg Message) error {
	return c.client.CommitUncommittedOffsets(ctx)
}

func (c *KafkaConsumer) Close() error {
	c.client.Close()
	return nil
}
