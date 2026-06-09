// Package worker holds types and logic for the KernelQ worker plane.
//
// This file implements ResultProducer with a real confluent-kafka-go client.
// After execution, workers can publish WorkerResultEvent JSON to
// kernelq.jobs.results for the control plane to consume.
package worker

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// resultFlushTimeoutMs is how long PublishResult waits for the broker to
// accept the message before returning a flush timeout error.
const resultFlushTimeoutMs = 5000

// KafkaResultProducer publishes WorkerResultEvent JSON to a Kafka results topic.
//
// It reuses KafkaProducerClient from kafka_dlq_producer.go so tests can inject
// the same fake client without connecting to a broker.
type KafkaResultProducer struct {
	Producer KafkaProducerClient
	Topic    string
}

// NewKafkaResultProducer creates a producer aimed at kernelq.jobs.results.
func NewKafkaResultProducer(bootstrapServers string) (*KafkaResultProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return &KafkaResultProducer{
		Producer: producer,
		Topic:    ResultTopic,
	}, nil
}

// PublishResult validates, JSON-encodes, and publishes one worker result event.
//
// This is synchronous for now: produce, then flush before returning.
// JobID becomes the Kafka message key so all results for one job land on the
// same partition when the topic uses key-based partitioning.
func (p KafkaResultProducer) PublishResult(event WorkerResultEvent) error {
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
		Key:   []byte(event.JobID),
		Value: []byte(jsonPayload),
	}

	if err := p.Producer.Produce(message, nil); err != nil {
		return fmt.Errorf("produce result event: %w", err)
	}

	remaining := p.Producer.Flush(resultFlushTimeoutMs)
	if remaining > 0 {
		return fmt.Errorf("flush result event: %d message(s) still in queue", remaining)
	}

	return nil
}
