package worker

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// fakeDispatchHandler records the last event passed to Handle.
type fakeDispatchHandler struct {
	received DispatchEvent
	called   bool
}

func (handler *fakeDispatchHandler) Handle(event DispatchEvent) (ExecutionResult, error) {
	handler.received = event
	handler.called = true
	return SuccessResult(), nil
}

// newKafkaMessage builds a broker record in memory (no real Kafka connection).
func newKafkaMessage(key string, value []byte) *kafka.Message {
	return &kafka.Message{
		Key:   []byte(key),
		Value: value,
	}
}

func TestProcessKafkaMessageReachesHandlerForValidMessage(t *testing.T) {
	handler := &fakeDispatchHandler{}
	consumer := KafkaConsumer{
		Runner: ConsumerRunner{Handler: handler},
	}

	err := consumer.ProcessKafkaMessage(newKafkaMessage("job-123", validDispatchJSON()))
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called, but it was not")
	}
}

func TestProcessKafkaMessageReturnsErrorForInvalidJSON(t *testing.T) {
	handler := &fakeDispatchHandler{}
	consumer := KafkaConsumer{
		Runner: ConsumerRunner{Handler: handler},
	}

	err := consumer.ProcessKafkaMessage(newKafkaMessage("job-123", []byte(`{"event_type":`)))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if handler.called {
		t.Fatal("handler should not be called for invalid JSON")
	}
}

func TestProcessKafkaMessageReturnsErrorForNilMessage(t *testing.T) {
	handler := &fakeDispatchHandler{}
	consumer := KafkaConsumer{
		Runner: ConsumerRunner{Handler: handler},
	}

	err := consumer.ProcessKafkaMessage(nil)
	if err == nil {
		t.Fatal("expected error for nil message, got nil")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("expected nil message error, got: %v", err)
	}
	if handler.called {
		t.Fatal("handler should not be called for nil message")
	}
}

func TestProcessKafkaMessageHandlerReceivesCorrectJobID(t *testing.T) {
	handler := &fakeDispatchHandler{}
	consumer := KafkaConsumer{
		Runner: ConsumerRunner{Handler: handler},
	}

	const jobID = "job-kafka-456"
	value := []byte(`{
		"event_type":"job.dispatch",
		"job_id":"job-kafka-456",
		"tenant_id":"tenant-a",
		"priority":5,
		"state":"dispatched",
		"payload":{"kind":"kafka-test"}
	}`)

	err := consumer.ProcessKafkaMessage(newKafkaMessage(jobID, value))
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if handler.received.JobID != jobID {
		t.Fatalf("expected job id %q, got %q", jobID, handler.received.JobID)
	}
}

// fakePoller returns a predefined list of events, then nil forever.
type fakePoller struct {
	events []kafka.Event
	index  int
	closed bool
}

func (poller *fakePoller) Poll(timeoutMs int) kafka.Event {
	if poller.index < len(poller.events) {
		event := poller.events[poller.index]
		poller.index++
		return event
	}
	return nil
}

func (poller *fakePoller) Close() error {
	poller.closed = true
	return nil
}

// runUntilEventsDelivered runs consumer.Run in a goroutine, waits until the
// fake poller has returned eventCount events, then cancels ctx so Run exits.
func runUntilEventsDelivered(t *testing.T, consumer *KafkaConsumer, poller *fakePoller, eventCount int) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, 100)
	}()

	for poller.index < eventCount {
		runtime.Gosched()
	}
	cancel()

	if err := <-done; err != nil {
		t.Fatalf("expected nil on cancel, got error: %v", err)
	}
}

func TestRunProcessesValidMessageAndIncrementsMessagesProcessed(t *testing.T) {
	handler := &fakeDispatchHandler{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-123", validDispatchJSON()),
		},
	}
	consumer := &KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: handler},
	}

	runUntilEventsDelivered(t, consumer, poller, 1)

	if !handler.called {
		t.Fatal("expected handler to be called")
	}
	if consumer.Stats.MessagesSeen != 1 {
		t.Fatalf("expected MessagesSeen 1, got %d", consumer.Stats.MessagesSeen)
	}
	if consumer.Stats.MessagesProcessed != 1 {
		t.Fatalf("expected MessagesProcessed 1, got %d", consumer.Stats.MessagesProcessed)
	}
	if consumer.Stats.MessageErrors != 0 {
		t.Fatalf("expected MessageErrors 0, got %d", consumer.Stats.MessageErrors)
	}
}

func TestRunIncrementsMessageErrorsForInvalidJSONAndContinues(t *testing.T) {
	handler := &fakeDispatchHandler{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-bad", []byte(`{"event_type":`)),
		},
	}
	consumer := &KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: handler},
	}

	runUntilEventsDelivered(t, consumer, poller, 1)

	if handler.called {
		t.Fatal("handler should not be called for invalid JSON")
	}
	if consumer.Stats.MessagesSeen != 1 {
		t.Fatalf("expected MessagesSeen 1, got %d", consumer.Stats.MessagesSeen)
	}
	if consumer.Stats.MessageErrors != 1 {
		t.Fatalf("expected MessageErrors 1, got %d", consumer.Stats.MessageErrors)
	}
	if consumer.Stats.MessagesProcessed != 0 {
		t.Fatalf("expected MessagesProcessed 0, got %d", consumer.Stats.MessagesProcessed)
	}
}

func TestRunProcessesValidMessageAfterInvalidMessage(t *testing.T) {
	handler := &fakeDispatchHandler{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-bad", []byte(`{"event_type":`)),
			newKafkaMessage("job-good", validDispatchJSON()),
		},
	}
	consumer := &KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: handler},
	}

	runUntilEventsDelivered(t, consumer, poller, 2)

	if !handler.called {
		t.Fatal("expected handler to be called for valid message after invalid")
	}
	if handler.received.JobID != "job-123" {
		t.Fatalf("expected job id job-123, got %q", handler.received.JobID)
	}
	if consumer.Stats.MessagesSeen != 2 {
		t.Fatalf("expected MessagesSeen 2, got %d", consumer.Stats.MessagesSeen)
	}
	if consumer.Stats.MessageErrors != 1 {
		t.Fatalf("expected MessageErrors 1, got %d", consumer.Stats.MessageErrors)
	}
	if consumer.Stats.MessagesProcessed != 1 {
		t.Fatalf("expected MessagesProcessed 1, got %d", consumer.Stats.MessagesProcessed)
	}
}

func TestRunIncrementsMessagesSeenForEveryKafkaMessage(t *testing.T) {
	handler := &fakeDispatchHandler{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-1", validDispatchJSON()),
			newKafkaMessage("job-2", []byte(`{"event_type":`)),
		},
	}
	consumer := &KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: handler},
	}

	runUntilEventsDelivered(t, consumer, poller, 2)

	if consumer.Stats.MessagesSeen != 2 {
		t.Fatalf("expected MessagesSeen 2, got %d", consumer.Stats.MessagesSeen)
	}
}

func TestRunReturnsErrorForKafkaErrorAndIncrementsKafkaErrors(t *testing.T) {
	brokerErr := kafka.NewError(kafka.ErrAllBrokersDown, "simulated broker error", false)
	poller := &fakePoller{
		events: []kafka.Event{brokerErr},
	}
	consumer := &KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: &fakeDispatchHandler{}},
	}

	err := consumer.Run(context.Background(), 100)
	if err == nil {
		t.Fatal("expected kafka error, got nil")
	}
	if consumer.Stats.KafkaErrors != 1 {
		t.Fatalf("expected KafkaErrors 1, got %d", consumer.Stats.KafkaErrors)
	}
	if err.Error() != brokerErr.Error() {
		t.Fatalf("expected %v, got %v", brokerErr, err)
	}
}

func TestRunClosesPollerOnContextCancellation(t *testing.T) {
	poller := &fakePoller{}
	consumer := &KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: &fakeDispatchHandler{}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := consumer.Run(ctx, 100)
	if err != nil {
		t.Fatalf("expected nil on cancel, got error: %v", err)
	}
	if !poller.closed {
		t.Fatal("expected poller to be closed on context cancel")
	}
}

// fakeDeadLetterProducer captures published DeadLetterEvent values for tests.
// Set err to simulate a publish failure.
type fakeDeadLetterProducer struct {
	events []DeadLetterEvent
	err    error
}

func (producer *fakeDeadLetterProducer) PublishDeadLetter(event DeadLetterEvent) error {
	if producer.err != nil {
		return producer.err
	}
	producer.events = append(producer.events, event)
	return nil
}

func newKafkaMessageWithTopic(topic, key string, value []byte) *kafka.Message {
	msg := newKafkaMessage(key, value)
	msg.TopicPartition.Topic = &topic
	return msg
}

// invalidDispatchEventJSON is valid JSON but fails DispatchEvent validation.
func invalidDispatchEventJSON() []byte {
	return []byte(`{
		"event_type":"job.retry",
		"job_id":"job-123",
		"tenant_id":"tenant-a",
		"priority":5,
		"state":"dispatched"
	}`)
}

func TestRunInvalidJSONPublishesOneDeadLetterEventWhenDLQConfigured(t *testing.T) {
	dlqProducer := &fakeDeadLetterProducer{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessageWithTopic("kernelq.jobs.dispatch", "job-bad", []byte(`{"event_type":`)),
		},
	}
	consumer := &KafkaConsumer{
		Poller:             poller,
		Runner:             ConsumerRunner{Handler: &fakeDispatchHandler{}},
		DeadLetterProducer: dlqProducer,
	}

	runUntilEventsDelivered(t, consumer, poller, 1)

	if len(dlqProducer.events) != 1 {
		t.Fatalf("expected 1 dead-letter event, got %d", len(dlqProducer.events))
	}
}

func TestRunInvalidDispatchEventPublishesOneDeadLetterEvent(t *testing.T) {
	dlqProducer := &fakeDeadLetterProducer{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessageWithTopic("kernelq.jobs.dispatch", "job-bad", invalidDispatchEventJSON()),
		},
	}
	consumer := &KafkaConsumer{
		Poller:             poller,
		Runner:             ConsumerRunner{Handler: &fakeDispatchHandler{}},
		DeadLetterProducer: dlqProducer,
	}

	runUntilEventsDelivered(t, consumer, poller, 1)

	if len(dlqProducer.events) != 1 {
		t.Fatalf("expected 1 dead-letter event, got %d", len(dlqProducer.events))
	}
	if !strings.Contains(dlqProducer.events[0].Reason, "event_type") {
		t.Fatalf("expected validation reason, got: %q", dlqProducer.events[0].Reason)
	}
}

func TestRunDeadLetterEventIncludesAllRequiredFields(t *testing.T) {
	const (
		topic   = "kernelq.jobs.dispatch"
		key     = "job-dlq-fields"
		payload = `{"event_type":`
	)
	dlqProducer := &fakeDeadLetterProducer{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessageWithTopic(topic, key, []byte(payload)),
		},
	}
	consumer := &KafkaConsumer{
		Poller:             poller,
		Runner:             ConsumerRunner{Handler: &fakeDispatchHandler{}},
		DeadLetterProducer: dlqProducer,
	}

	runUntilEventsDelivered(t, consumer, poller, 1)

	event := dlqProducer.events[0]
	if event.Reason == "" {
		t.Fatal("expected non-empty reason")
	}
	if event.OriginalKey != key {
		t.Fatalf("expected original key %q, got %q", key, event.OriginalKey)
	}
	if event.OriginalValue != payload {
		t.Fatalf("expected original value %q, got %q", payload, event.OriginalValue)
	}
	if event.SourceTopic != topic {
		t.Fatalf("expected source topic %q, got %q", topic, event.SourceTopic)
	}
	if event.Worker != "kernelq-go-worker" {
		t.Fatalf("expected worker kernelq-go-worker, got %q", event.Worker)
	}
}

func TestRunDeadLettersPublishedIncrementsOnSuccessfulDLQPublish(t *testing.T) {
	dlqProducer := &fakeDeadLetterProducer{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-bad", []byte(`{"event_type":`)),
		},
	}
	consumer := &KafkaConsumer{
		Poller:             poller,
		Runner:             ConsumerRunner{Handler: &fakeDispatchHandler{}},
		DeadLetterProducer: dlqProducer,
	}

	runUntilEventsDelivered(t, consumer, poller, 1)

	if consumer.Stats.DeadLettersPublished != 1 {
		t.Fatalf("expected DeadLettersPublished 1, got %d", consumer.Stats.DeadLettersPublished)
	}
	if consumer.Stats.DeadLetterPublishErrors != 0 {
		t.Fatalf("expected DeadLetterPublishErrors 0, got %d", consumer.Stats.DeadLetterPublishErrors)
	}
}

func TestRunDeadLetterPublishErrorsIncrementsWhenProducerFails(t *testing.T) {
	dlqProducer := &fakeDeadLetterProducer{err: fmt.Errorf("simulated dlq publish failure")}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-bad", []byte(`{"event_type":`)),
		},
	}
	consumer := &KafkaConsumer{
		Poller:             poller,
		Runner:             ConsumerRunner{Handler: &fakeDispatchHandler{}},
		DeadLetterProducer: dlqProducer,
	}

	runUntilEventsDelivered(t, consumer, poller, 1)

	if consumer.Stats.DeadLetterPublishErrors != 1 {
		t.Fatalf("expected DeadLetterPublishErrors 1, got %d", consumer.Stats.DeadLetterPublishErrors)
	}
	if consumer.Stats.DeadLettersPublished != 0 {
		t.Fatalf("expected DeadLettersPublished 0, got %d", consumer.Stats.DeadLettersPublished)
	}
}

func TestRunWithoutDLQProducerIncrementsMessageErrorsOnly(t *testing.T) {
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-bad", []byte(`{"event_type":`)),
		},
	}
	consumer := &KafkaConsumer{
		Poller:             poller,
		Runner:             ConsumerRunner{Handler: &fakeDispatchHandler{}},
		DeadLetterProducer: nil,
	}

	runUntilEventsDelivered(t, consumer, poller, 1)

	if consumer.Stats.MessageErrors != 1 {
		t.Fatalf("expected MessageErrors 1, got %d", consumer.Stats.MessageErrors)
	}
	if consumer.Stats.DeadLettersPublished != 0 {
		t.Fatalf("expected DeadLettersPublished 0, got %d", consumer.Stats.DeadLettersPublished)
	}
	if consumer.Stats.DeadLetterPublishErrors != 0 {
		t.Fatalf("expected DeadLetterPublishErrors 0, got %d", consumer.Stats.DeadLetterPublishErrors)
	}
}

func TestEnqueueKafkaMessageIncrementsQueueFullErrorsWhenQueueIsFull(t *testing.T) {
	release := make(chan struct{})
	executor := newBlockingExecutor(release, 2)
	handler := handlerWithExecutor(executor)
	consumer := &KafkaConsumer{
		Runner:        ConsumerRunner{Handler: handler},
		WorkerCount:   1,
		QueueCapacity: 1,
	}

	pool := NewWorkerPool(1, 1, handler, nil, nil)
	pool.Start()
	defer func() {
		close(release)
		pool.Shutdown()
	}()

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-1", validDispatchJSON()))

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to start first job")
	}

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-2", validDispatchJSON()))
	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-3", validDispatchJSON()))

	if consumer.Stats.MessagesSeen != 3 {
		t.Fatalf("expected MessagesSeen 3, got %d", consumer.Stats.MessagesSeen)
	}
	if consumer.Stats.QueueFullErrors != 1 {
		t.Fatalf("expected QueueFullErrors 1, got %d", consumer.Stats.QueueFullErrors)
	}
	if consumer.Stats.MessageErrors != 0 {
		t.Fatalf("expected MessageErrors 0, got %d", consumer.Stats.MessageErrors)
	}
}

func TestEnqueueKafkaMessageQueueFullDoesNotPublishDeadLetter(t *testing.T) {
	release := make(chan struct{})
	executor := newBlockingExecutor(release, 2)
	handler := handlerWithExecutor(executor)
	dlqProducer := &fakeDeadLetterProducer{}
	consumer := &KafkaConsumer{
		Runner:             ConsumerRunner{Handler: handler},
		DeadLetterProducer: dlqProducer,
	}

	pool := NewWorkerPool(1, 1, handler, nil, nil)
	pool.Start()
	defer func() {
		close(release)
		pool.Shutdown()
	}()

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-1", validDispatchJSON()))

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to start first job")
	}

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-2", validDispatchJSON()))
	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-3", validDispatchJSON()))

	if len(dlqProducer.events) != 0 {
		t.Fatalf("expected no dead-letter events for queue full, got %d", len(dlqProducer.events))
	}
	if consumer.Stats.QueueFullErrors != 1 {
		t.Fatalf("expected QueueFullErrors 1, got %d", consumer.Stats.QueueFullErrors)
	}
}

func TestRunRejectsNonPositivePollTimeout(t *testing.T) {
	consumer := &KafkaConsumer{
		Poller: &fakePoller{},
		Runner: ConsumerRunner{Handler: &fakeDispatchHandler{}},
	}

	err := consumer.Run(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error for zero poll timeout, got nil")
	}
	if !strings.Contains(err.Error(), "poll timeout") {
		t.Fatalf("expected poll timeout error, got: %v", err)
	}
}
