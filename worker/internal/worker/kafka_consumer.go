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
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// workerIdentity is stored on DeadLetterEvent records so operators know
// which worker process rejected a message.
const workerIdentity = "kernelq-go-worker"

// queueFullEnqueueRetryDelay is how long the poll loop waits before one retry
// when the bounded work queue rejects a job. Kafka pause/resume is future work.
var queueFullEnqueueRetryDelay = 50 * time.Millisecond

// sleepQueueFullRetry backs off before a queue-full retry (overridable in tests).
var sleepQueueFullRetry = time.Sleep

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
	WorkQueueCapacity       int
	WorkQueueDepth          int
	WorkItemsEnqueued       int
	WorkQueueFullErrors     int
	BackpressurePauseEvents int
	BackpressureResumeEvents int
	DeadLettersPublished    int
	DeadLetterPublishErrors int
	DuplicateExecutions     int
	IdempotencyErrors       int
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
	// QueueCapacity sets the bounded work-queue size in Run (0 => DefaultQueueCapacity).
	QueueCapacity int
	// DeadLetterProducer is optional. When set, processing failures publish
	// a DeadLetterEvent before the poll loop continues.
	DeadLetterProducer DeadLetterProducer
	// BackpressurePolicy is optional. When set with PauseResumeController,
	// Run evaluates queue depth against watermarks and drives pause/resume.
	BackpressurePolicy *BackpressurePolicy
	// PauseResumeController is optional. Use InMemoryPauseResumeController in
	// tests; a Kafka adapter will call broker Pause/Resume later.
	PauseResumeController PauseResumeController
	Stats              ConsumerStats
	statsMu            sync.Mutex
}

// ProcessKafkaMessage handles one record from the Kafka client.
func (c KafkaConsumer) ProcessKafkaMessage(ctx context.Context, msg *kafka.Message) error {
	if msg == nil {
		return fmt.Errorf("kafka message is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var offset int64
	if msg.TopicPartition.Offset >= 0 {
		offset = int64(msg.TopicPartition.Offset)
	}

	message := Message{
		Key:       string(msg.Key),
		Value:     msg.Value,
		Headers:   append([]kafka.Header(nil), msg.Headers...),
		Topic:     sourceTopicFromMessage(msg),
		Partition: msg.TopicPartition.Partition,
		Offset:    offset,
	}

	if err := c.Runner.ProcessMessage(ctx, message); err != nil {
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

	var pool *WorkerPool
	pool = NewWorkerPoolWithContext(
		ctx,
		c.WorkerCount,
		c.QueueCapacity,
		c.Runner.Handler,
		func(workerID string, item WorkItem) {
			c.recordMessageProcessed(workerID, item)
			c.maybeApplyBackpressure(pool)
		},
		func(workerID string, item WorkItem, err error) {
			c.handleWorkItemError(workerID, item, err)
		},
	)
	pool.Logger = c.Runner.Logger
	c.recordWorkQueueCapacity(pool.QueueCapacity())
	pool.Start()
	defer func() {
		c.recordWorkQueueDepth(pool.QueueDepth())
		pool.Shutdown()
	}()

	for {
		if ctx.Err() != nil {
			_ = c.Poller.Close()
			return nil
		}

		c.maybeApplyBackpressure(pool)

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

	var offset int64
	if msg.TopicPartition.Offset >= 0 {
		offset = int64(msg.TopicPartition.Offset)
	}

	item := WorkItem{
		Event:         event,
		OriginalKey:   string(msg.Key),
		OriginalValue: msg.Value,
		SourceTopic:   sourceTopicFromMessage(msg),
		Headers:       append([]kafka.Header(nil), msg.Headers...),
		Partition:     msg.TopicPartition.Partition,
		Offset:        offset,
	}

	if err := pool.Enqueue(item); err != nil {
		if err != ErrWorkerQueueFull {
			return
		}
		c.handleQueueFull(event, pool.QueueCapacity(), err)
		sleepQueueFullRetry(queueFullEnqueueRetryDelay)
		if err := pool.Enqueue(item); err != nil {
			return
		}
	}
	c.incWorkItemsEnqueued()
	c.maybeApplyBackpressure(pool)
}

// maybeApplyBackpressure records queue depth and, when policy and controller are
// both configured, drives pause/resume from watermark thresholds.
func (c *KafkaConsumer) maybeApplyBackpressure(pool *WorkerPool) {
	depth := pool.QueueDepth()
	capacity := pool.QueueCapacity()
	c.recordWorkQueueDepth(depth)

	if c.BackpressurePolicy == nil || c.PauseResumeController == nil {
		return
	}

	policy := *c.BackpressurePolicy
	controller := c.PauseResumeController

	if !controller.IsPaused() && policy.ShouldPause(depth, capacity) {
		if err := controller.Pause(); err != nil {
			return
		}
		c.incBackpressurePauseEvents()
		fmt.Printf(
			"event=worker_backpressure_pause queue_depth=%d queue_capacity=%d\n",
			depth,
			capacity,
		)
		return
	}

	if controller.IsPaused() && policy.ShouldResume(depth, capacity) {
		if err := controller.Resume(); err != nil {
			return
		}
		c.incBackpressureResumeEvents()
		fmt.Printf(
			"event=worker_backpressure_resume queue_depth=%d queue_capacity=%d\n",
			depth,
			capacity,
		)
	}
}

// handleQueueFull records saturation when the bounded worker queue rejects a job.
//
// This is backpressure at enqueue time, not a processing failure — no DLQ yet.
// Proactive pause is handled by maybeApplyBackpressure when policy is wired.
func (c *KafkaConsumer) handleQueueFull(event DispatchEvent, queueCapacity int, err error) {
	if err != ErrWorkerQueueFull {
		return
	}
	c.incWorkQueueFullErrors()
	fmt.Printf(
		"event=worker_queue_full job_id=%s queue_capacity=%d\n",
		event.JobID,
		queueCapacity,
	)
}

func (c *KafkaConsumer) recordWorkQueueCapacity(capacity int) {
	c.statsMu.Lock()
	c.Stats.WorkQueueCapacity = capacity
	c.statsMu.Unlock()
}

func (c *KafkaConsumer) recordWorkQueueDepth(depth int) {
	c.statsMu.Lock()
	c.Stats.WorkQueueDepth = depth
	c.statsMu.Unlock()
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

func (c *KafkaConsumer) incWorkItemsEnqueued() {
	c.statsMu.Lock()
	c.Stats.WorkItemsEnqueued++
	c.statsMu.Unlock()
}

func (c *KafkaConsumer) incWorkQueueFullErrors() {
	c.statsMu.Lock()
	c.Stats.WorkQueueFullErrors++
	c.statsMu.Unlock()
}

func (c *KafkaConsumer) incBackpressurePauseEvents() {
	c.statsMu.Lock()
	c.Stats.BackpressurePauseEvents++
	c.statsMu.Unlock()
}

func (c *KafkaConsumer) incBackpressureResumeEvents() {
	c.statsMu.Lock()
	c.Stats.BackpressureResumeEvents++
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

	fmt.Printf("event=message_processing_error worker=%s error=%q\n", workerName, processingErr.Error())

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
