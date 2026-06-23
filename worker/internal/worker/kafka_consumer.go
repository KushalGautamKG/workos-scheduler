// Package worker holds types and logic for the KernelQ worker plane.
//
// This file adapts confluent-kafka-go records into our in-memory Message type.
// KafkaConsumer connects broker polling to ConsumerRunner without mixing
// transport into parsing or execution logic.
package worker

import (
	"context"
	"fmt"
	"sync"

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
	// WorkerCount sets pool size in Run (0 => DefaultWorkerCount).
	WorkerCount int
	// DeadLetterProducer is optional. When set, processing failures publish
	// a DeadLetterEvent before the poll loop continues.
	DeadLetterProducer DeadLetterProducer
	Stats              ConsumerStats
	statsMu            sync.Mutex
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
// Valid messages are decoded on the poll goroutine, enqueued to a worker pool,
// and executed concurrently. Invalid or unprocessable messages are handled on
// the poll goroutine (DLQ + stats) as before.
//
// ProcessKafkaMessage remains synchronous for tests and one-off use.
func (c *KafkaConsumer) Run(ctx context.Context, pollTimeoutMs int) error {
	if pollTimeoutMs <= 0 {
		return fmt.Errorf("poll timeout must be positive, got %d", pollTimeoutMs)
	}

	if c.Runner.Handler == nil {
		return fmt.Errorf("dispatch handler is not configured")
	}

	pool := NewWorkerPool(
		c.WorkerCount,
		c.Runner.Handler,
		func(workerID string, item WorkItem) {
			c.recordMessageProcessed(workerID, item)
		},
		func(workerID string, item WorkItem, err error) {
			c.handleWorkItemError(workerID, item, err)
		},
	)
	pool.Start()
	defer pool.Shutdown()

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
			c.enqueueKafkaMessage(pool, e)
		case kafka.Error:
			// Broker/client problems are still fatal for now.
			c.incKafkaErrors()
			return e
		}
	}
}

// enqueueKafkaMessage decodes one record and hands it to the worker pool.
func (c *KafkaConsumer) enqueueKafkaMessage(pool *WorkerPool, msg *kafka.Message) {
	c.incMessagesSeen()

	event, err := ParseDispatchEvent(msg.Value)
	if err != nil {
		c.handleProcessingError(msg, err)
		return
	}

	pool.Enqueue(WorkItem{
		Event:         event,
		OriginalKey:   string(msg.Key),
		OriginalValue: msg.Value,
		SourceTopic:   sourceTopicFromMessage(msg),
	})
}

func (c *KafkaConsumer) recordMessageProcessed(workerID string, item WorkItem) {
	_ = workerID
	_ = item
	c.statsMu.Lock()
	c.Stats.MessagesProcessed++
	c.statsMu.Unlock()
}

func (c *KafkaConsumer) incMessagesSeen() {
	c.statsMu.Lock()
	c.Stats.MessagesSeen++
	c.statsMu.Unlock()
}

func (c *KafkaConsumer) incKafkaErrors() {
	c.statsMu.Lock()
	c.Stats.KafkaErrors++
	c.statsMu.Unlock()
}

func (c *KafkaConsumer) handleWorkItemError(workerID string, item WorkItem, processingErr error) {
	msg := &kafka.Message{
		Key:   []byte(item.OriginalKey),
		Value: item.OriginalValue,
	}
	if item.SourceTopic != "" {
		topic := item.SourceTopic
		msg.TopicPartition.Topic = &topic
	}

	c.handleProcessingErrorWithWorker(msg, processingErr, workerNameForPoolWorker(workerID))
}

// handleProcessingError records a message failure and optionally publishes
// a dead-letter event. The poll loop always continues afterward.
func (c *KafkaConsumer) handleProcessingError(msg *kafka.Message, processingErr error) {
	c.handleProcessingErrorWithWorker(msg, processingErr, workerIdentity)
}

func (c *KafkaConsumer) handleProcessingErrorWithWorker(
	msg *kafka.Message,
	processingErr error,
	workerName string,
) {
	c.statsMu.Lock()
	c.Stats.MessageErrors++
	c.statsMu.Unlock()

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
		Worker:        workerName,
	}

	if err := c.DeadLetterProducer.PublishDeadLetter(event); err != nil {
		c.statsMu.Lock()
		c.Stats.DeadLetterPublishErrors++
		c.statsMu.Unlock()
		return
	}

	c.statsMu.Lock()
	c.Stats.DeadLettersPublished++
	c.statsMu.Unlock()
}

func workerNameForPoolWorker(workerID string) string {
	return fmt.Sprintf("%s/%s", workerIdentity, workerID)
}

// sourceTopicFromMessage reads the topic name from a Kafka message when the
// broker attached TopicPartition metadata. Tests may omit it.
func sourceTopicFromMessage(msg *kafka.Message) string {
	if msg.TopicPartition.Topic != nil && *msg.TopicPartition.Topic != "" {
		return *msg.TopicPartition.Topic
	}
	return "unknown"
}
