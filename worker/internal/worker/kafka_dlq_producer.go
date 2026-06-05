// Package worker holds types and logic for the KernelQ worker plane.
//
// This file implements DeadLetterProducer with a real confluent-kafka-go
// client. Invalid dispatch messages can be published to kernelq.jobs.dlq.
package worker

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// dlqFlushTimeoutMs is how long PublishDeadLetter waits for the broker to
// accept the message before returning a flush timeout error.
const dlqFlushTimeoutMs = 5000

// KafkaProducerClient is the small Kafka surface needed for DLQ publishing.
//
// *kafka.Producer satisfies this interface. Tests can inject a fake client.
type KafkaProducerClient interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Flush(timeoutMs int) int
}

// KafkaDeadLetterProducer publishes DeadLetterEvent JSON to a Kafka DLQ topic.
type KafkaDeadLetterProducer struct {
	Producer KafkaProducerClient
	Topic    string
}

// NewKafkaDeadLetterProducer creates a producer aimed at kernelq.jobs.dlq.
func NewKafkaDeadLetterProducer(bootstrapServers string) (*KafkaDeadLetterProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return &KafkaDeadLetterProducer{
		Producer: producer,
		Topic:    DLQTopic,
	}, nil
}

// PublishDeadLetter validates, JSON-encodes, and publishes one dead-letter
// event. This is synchronous for now: produce, then flush before returning.
func (p KafkaDeadLetterProducer) PublishDeadLetter(event DeadLetterEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}

	jsonPayload, err := event.ToJSON()
	if err != nil {
		return err
	}

	topic := p.Topic
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(event.OriginalKey),
		Value: []byte(jsonPayload),
	}

	if err := p.Producer.Produce(message, nil); err != nil {
		return fmt.Errorf("produce dead-letter event: %w", err)
	}

	remaining := p.Producer.Flush(dlqFlushTimeoutMs)
	if remaining > 0 {
		return fmt.Errorf("flush dead-letter event: %d message(s) still in queue", remaining)
	}

	return nil
}
