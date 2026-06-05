// Package worker holds types and logic for the KernelQ worker plane.
//
// This file adapts confluent-kafka-go records into our in-memory Message type.
// KafkaConsumer connects broker polling to ConsumerRunner without mixing
// transport into parsing or execution logic.
package worker

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// workerIdentity is stored on DeadLetterEvent records so operators know
// which worker process rejected a message.
const workerIdentity = "kernelq-go-worker"

// KafkaPoller is the small broker surface Run needs for polling.
//
// *kafka.Consumer satisfies this interface. Tests can use a fake poller
// without connecting to a real broker.
type KafkaPoller interface {
	Poll(timeoutMs int) kafka.Event
	Close() error
}

// ConsumerStats counts poll-loop outcomes for observability.
//
// Future metrics can expose these fields (messages polled, errors, and so on).
type ConsumerStats struct {
	MessagesSeen            int
	MessagesProcessed       int
	MessageErrors           int
	KafkaErrors             int
	DeadLettersPublished    int
	DeadLetterPublishErrors int
}

// KafkaConsumer wraps a broker poller and our message-processing runner.
//
// The Kafka SDK delivers *kafka.Message values; ConsumerRunner knows how to
// parse, validate, and hand off to a DispatchHandler. This struct connects
// the two layers without mixing broker code into business logic.
type KafkaConsumer struct {
	Poller KafkaPoller
	Runner ConsumerRunner
	// DeadLetterProducer is optional. When set, processing failures publish
	// a DeadLetterEvent before the poll loop continues.
	DeadLetterProducer DeadLetterProducer
	Stats              ConsumerStats
}

// ProcessKafkaMessage handles one record from the Kafka client.
func (c KafkaConsumer) ProcessKafkaMessage(msg *kafka.Message) error {
	if msg == nil {
		return fmt.Errorf("kafka message is nil")
	}

	// Map broker record fields onto our simple Message type.
	message := Message{
		Key:   string(msg.Key),
		Value: msg.Value,
	}

	if err := c.Runner.ProcessMessage(message); err != nil {
		return err
	}

	return nil
}

// Run polls the broker until ctx is canceled.
//
// Invalid or unprocessable messages increment MessageErrors and keep polling.
// When DeadLetterProducer is configured, failures also publish a DeadLetterEvent.
// Offset commits and retries are not implemented yet.
func (c *KafkaConsumer) Run(ctx context.Context, pollTimeoutMs int) error {
	if pollTimeoutMs <= 0 {
		return fmt.Errorf("poll timeout must be positive, got %d", pollTimeoutMs)
	}

	for {
		if ctx.Err() != nil {
			_ = c.Poller.Close()
			return nil
		}

		event := c.Poller.Poll(pollTimeoutMs)

		if ctx.Err() != nil {
			_ = c.Poller.Close()
			return nil
		}

		if event == nil {
			continue
		}

		switch e := event.(type) {
		case *kafka.Message:
			c.Stats.MessagesSeen++
			if err := c.ProcessKafkaMessage(e); err != nil {
				// Bad JSON, validation failures, and handler errors must not
				// stop the worker. Count the failure and optionally DLQ it.
				c.handleProcessingError(e, err)
				continue
			}
			c.Stats.MessagesProcessed++
		case kafka.Error:
			// Broker/client problems are still fatal for now.
			c.Stats.KafkaErrors++
			return e
		}
	}
}

// handleProcessingError records a message failure and optionally publishes
// a dead-letter event. The poll loop always continues afterward.
func (c *KafkaConsumer) handleProcessingError(msg *kafka.Message, processingErr error) {
	c.Stats.MessageErrors++

	// No DLQ producer wired (common in tests or gradual rollout).
	if c.DeadLetterProducer == nil {
		return
	}

	// Preserve the original Kafka record plus why processing failed.
	event := DeadLetterEvent{
		Reason:        processingErr.Error(),
		OriginalKey:   string(msg.Key),
		OriginalValue: string(msg.Value),
		SourceTopic:   sourceTopicFromMessage(msg),
		Worker:        workerIdentity,
	}

	if err := c.DeadLetterProducer.PublishDeadLetter(event); err != nil {
		c.Stats.DeadLetterPublishErrors++
		return
	}

	c.Stats.DeadLettersPublished++
}

// sourceTopicFromMessage reads the topic name from a Kafka message when the
// broker attached TopicPartition metadata. Tests may omit it.
func sourceTopicFromMessage(msg *kafka.Message) string {
	if msg.TopicPartition.Topic != nil && *msg.TopicPartition.Topic != "" {
		return *msg.TopicPartition.Topic
	}
	return "unknown"
}
