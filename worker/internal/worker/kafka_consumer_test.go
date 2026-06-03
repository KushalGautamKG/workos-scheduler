package worker

import (
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
