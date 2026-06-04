package worker

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// fakeDispatchHandler records the last event passed to Handle.
type fakeDispatchHandler struct {
	received DispatchEvent
	called   bool
}

func (handler *fakeDispatchHandler) Handle(event DispatchEvent) error {
	handler.received = event
	handler.called = true
	return nil
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

func TestRunProcessesOneKafkaMessageAndCallsHandler(t *testing.T) {
	handler := &fakeDispatchHandler{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-123", validDispatchJSON()),
		},
	}
	consumer := KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: handler},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, 100)
	}()

	for !handler.called {
		runtime.Gosched()
	}
	cancel()

	err := <-done
	if err != nil {
		t.Fatalf("expected nil on cancel, got error: %v", err)
	}
	if !handler.called {
		t.Fatal("expected handler to be called")
	}
}

func TestRunReturnsNilWhenContextIsCanceled(t *testing.T) {
	poller := &fakePoller{}
	consumer := KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: &fakeDispatchHandler{}},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := consumer.Run(ctx, 100)
	if err != nil {
		t.Fatalf("expected nil on canceled context, got error: %v", err)
	}
}

func TestRunReturnsErrorForInvalidMessage(t *testing.T) {
	handler := &fakeDispatchHandler{}
	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-123", []byte(`{"event_type":`)),
		},
	}
	consumer := KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: handler},
	}

	ctx := context.Background()
	err := consumer.Run(ctx, 100)
	if err == nil {
		t.Fatal("expected error for invalid message, got nil")
	}
	if handler.called {
		t.Fatal("handler should not be called for invalid message")
	}
}

func TestRunReturnsErrorForKafkaError(t *testing.T) {
	brokerErr := kafka.NewError(kafka.ErrAllBrokersDown, "simulated broker error", false)
	poller := &fakePoller{
		events: []kafka.Event{brokerErr},
	}
	consumer := KafkaConsumer{
		Poller: poller,
		Runner: ConsumerRunner{Handler: &fakeDispatchHandler{}},
	}

	err := consumer.Run(context.Background(), 100)
	if err == nil {
		t.Fatal("expected kafka error, got nil")
	}
	if err.Error() != brokerErr.Error() {
		t.Fatalf("expected %v, got %v", brokerErr, err)
	}
}

func TestRunRejectsNonPositivePollTimeout(t *testing.T) {
	consumer := KafkaConsumer{
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

func TestRunClosesPollerWhenContextIsCanceled(t *testing.T) {
	poller := &fakePoller{}
	consumer := KafkaConsumer{
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
