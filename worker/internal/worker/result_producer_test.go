package worker

import (
	"context"
	"strings"
	"testing"
)

func TestRecordingResultProducerStoresValidWorkerResultEvent(t *testing.T) {
	producer := &RecordingResultProducer{}
	event := validWorkerResultEvent()

	err := producer.PublishResult(context.Background(), event)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if len(producer.Published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(producer.Published))
	}
	if producer.Published[0].JobID != event.JobID {
		t.Fatalf("expected job_id %q, got %q", event.JobID, producer.Published[0].JobID)
	}
}

func TestRecordingResultProducerReturnsErrorForInvalidEvent(t *testing.T) {
	producer := &RecordingResultProducer{}
	event := validWorkerResultEvent()
	event.JobID = "   "

	err := producer.PublishResult(context.Background(), event)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "job_id") {
		t.Fatalf("expected job_id error, got: %v", err)
	}
}

func TestRecordingResultProducerDoesNotAppendInvalidEvent(t *testing.T) {
	producer := &RecordingResultProducer{}
	event := validWorkerResultEvent()
	event.EventType = "job.dispatch"

	_ = producer.PublishResult(context.Background(), event)

	if len(producer.Published) != 0 {
		t.Fatalf("expected 0 published events, got %d", len(producer.Published))
	}
}

func TestRecordingResultProducerStoresMultipleValidEventsInOrder(t *testing.T) {
	producer := &RecordingResultProducer{}

	first := NewWorkerResultEvent("job-1", SuccessResult(), testWorkerIdentity)
	second := NewWorkerResultEvent("job-2", RetryableFailureResult("timeout"), testWorkerIdentity)
	third := NewWorkerResultEvent("job-3", TerminalFailureResult("bad payload"), testWorkerIdentity)

	for _, event := range []WorkerResultEvent{first, second, third} {
		if err := producer.PublishResult(context.Background(), event); err != nil {
			t.Fatalf("expected success for job %q, got error: %v", event.JobID, err)
		}
	}

	if len(producer.Published) != 3 {
		t.Fatalf("expected 3 published events, got %d", len(producer.Published))
	}
	if producer.Published[0].JobID != "job-1" {
		t.Fatalf("expected first job_id job-1, got %q", producer.Published[0].JobID)
	}
	if producer.Published[1].JobID != "job-2" {
		t.Fatalf("expected second job_id job-2, got %q", producer.Published[1].JobID)
	}
	if producer.Published[2].JobID != "job-3" {
		t.Fatalf("expected third job_id job-3, got %q", producer.Published[2].JobID)
	}
}
