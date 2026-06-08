package worker

import (
	"encoding/json"
	"strings"
	"testing"
)

const testWorkerIdentity = "kernelq-go-worker"

func validWorkerResultEvent() WorkerResultEvent {
	return NewWorkerResultEvent("job-123", SuccessResult(), testWorkerIdentity)
}

func TestNewWorkerResultEventBuildsEventFromSuccessResult(t *testing.T) {
	event := NewWorkerResultEvent("job-123", SuccessResult(), testWorkerIdentity)

	if event.EventType != "job.result" {
		t.Fatalf("expected event_type %q, got %q", "job.result", event.EventType)
	}
	if event.JobID != "job-123" {
		t.Fatalf("expected job_id %q, got %q", "job-123", event.JobID)
	}
	if event.Status != ExecutionSucceeded {
		t.Fatalf("expected status %q, got %q", ExecutionSucceeded, event.Status)
	}
	if event.Worker != testWorkerIdentity {
		t.Fatalf("expected worker %q, got %q", testWorkerIdentity, event.Worker)
	}
}

func TestNewWorkerResultEventBuildsEventFromRetryableFailureResult(t *testing.T) {
	result := RetryableFailureResult("dependency timeout")
	event := NewWorkerResultEvent("job-456", result, testWorkerIdentity)

	if event.EventType != "job.result" {
		t.Fatalf("expected event_type %q, got %q", "job.result", event.EventType)
	}
	if event.JobID != "job-456" {
		t.Fatalf("expected job_id %q, got %q", "job-456", event.JobID)
	}
	if event.Status != ExecutionRetryableFailure {
		t.Fatalf("expected status %q, got %q", ExecutionRetryableFailure, event.Status)
	}
	if event.Message != "dependency timeout" {
		t.Fatalf("expected message %q, got %q", "dependency timeout", event.Message)
	}
}

func TestWorkerResultEventValidateRejectsWrongEventType(t *testing.T) {
	event := validWorkerResultEvent()
	event.EventType = "job.dispatch"

	err := event.Validate()
	if err == nil {
		t.Fatal("expected error for wrong event_type, got nil")
	}
	if !strings.Contains(err.Error(), "event_type") {
		t.Fatalf("expected event_type error, got: %v", err)
	}
}

func TestWorkerResultEventValidateRejectsBlankJobID(t *testing.T) {
	event := validWorkerResultEvent()
	event.JobID = "   "

	err := event.Validate()
	if err == nil {
		t.Fatal("expected error for blank job_id, got nil")
	}
	if !strings.Contains(err.Error(), "job_id") {
		t.Fatalf("expected job_id error, got: %v", err)
	}
}

func TestWorkerResultEventValidateRejectsInvalidStatus(t *testing.T) {
	event := validWorkerResultEvent()
	event.Status = ExecutionStatus("unknown")

	err := event.Validate()
	if err == nil {
		t.Fatal("expected error for invalid status, got nil")
	}
	if !strings.Contains(err.Error(), "status") {
		t.Fatalf("expected status error, got: %v", err)
	}
}

func TestWorkerResultEventValidateRejectsBlankWorker(t *testing.T) {
	event := validWorkerResultEvent()
	event.Worker = "   "

	err := event.Validate()
	if err == nil {
		t.Fatal("expected error for blank worker, got nil")
	}
	if !strings.Contains(err.Error(), "worker") {
		t.Fatalf("expected worker error, got: %v", err)
	}
}

func TestWorkerResultEventToJSONReturnsExpectedFields(t *testing.T) {
	event := validWorkerResultEvent()

	jsonText, err := event.ToJSON()
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	var decoded map[string]string
	if err := json.Unmarshal([]byte(jsonText), &decoded); err != nil {
		t.Fatalf("expected valid JSON, got parse error: %v", err)
	}

	if decoded["event_type"] != "job.result" {
		t.Fatalf("expected event_type job.result, got %q", decoded["event_type"])
	}
	if decoded["job_id"] != event.JobID {
		t.Fatalf("expected job_id %q, got %q", event.JobID, decoded["job_id"])
	}
	if decoded["status"] != string(ExecutionSucceeded) {
		t.Fatalf("expected status %q, got %q", ExecutionSucceeded, decoded["status"])
	}
	if decoded["worker"] != testWorkerIdentity {
		t.Fatalf("expected worker %q, got %q", testWorkerIdentity, decoded["worker"])
	}
}

func TestWorkerResultEventValidateAllowsBlankMessage(t *testing.T) {
	event := NewWorkerResultEvent("job-123", SuccessResult(), testWorkerIdentity)
	event.Message = ""

	if err := event.Validate(); err != nil {
		t.Fatalf("expected blank message to be allowed, got error: %v", err)
	}
}
