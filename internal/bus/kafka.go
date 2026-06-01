package bus

import (
	"context"
	"fmt"
	"strconv"

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
type KafkaConsumer struct {
	client *kgo.Client
}

// NewKafkaConsumer creates a Kafka consumer in the given group.
func NewKafkaConsumer(brokers []string, groupID string) (*KafkaConsumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
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
	fetches := c.client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return Message{}, fmt.Errorf("kafka consumer closed")
	}
	if err := fetches.Err(); err != nil {
		return Message{}, fmt.Errorf("kafka poll: %w", err)
	}
	var msg Message
	fetches.EachRecord(func(r *kgo.Record) {
		headers := make(map[string]string, len(r.Headers))
		for _, h := range r.Headers {
			headers[h.Key] = string(h.Value)
		}
		if seq, err := strconv.ParseUint(headers["seq-num"], 10, 64); err == nil {
			msg.SeqNum = seq
		}
		msg = Message{
			Topic:     r.Topic,
			Key:       string(r.Key),
			Value:     r.Value,
			Headers:   headers,
			Offset:    r.Offset,
			Partition: r.Partition,
			SeqNum:    msg.SeqNum,
		}
	})
	return msg, nil
}

func (c *KafkaConsumer) Commit(ctx context.Context, msg Message) error {
	return c.client.CommitUncommittedOffsets(ctx)
}

func (c *KafkaConsumer) Close() error {
	c.client.Close()
	return nil
}
