package worker

import (
	"encoding/json"
	"strings"
	"testing"
)

func validDeadLetterEvent() DeadLetterEvent {
	return DeadLetterEvent{
		Reason:        "invalid json",
		OriginalKey:   "job-123",
		OriginalValue: `{"event_type":"job.dispatch"`,
		SourceTopic:   "kernelq.jobs.dispatch",
		Worker:        "kernelq-worker",
	}
}

func TestDeadLetterEventValidateSucceedsForValidEvent(t *testing.T) {
	err := validDeadLetterEvent().Validate()
	if err != nil {
		t.Fatalf("expected valid dead-letter event, got error: %v", err)
	}
}

func TestDeadLetterEventValidateFailsForBlankReason(t *testing.T) {
	event := validDeadLetterEvent()
	event.Reason = "   "

	err := event.Validate()
	if err == nil {
		t.Fatal("expected error for blank reason, got nil")
	}
	if !strings.Contains(err.Error(), "reason") {
		t.Fatalf("expected reason error, got: %v", err)
	}
}

func TestDeadLetterEventValidateFailsForBlankOriginalValue(t *testing.T) {
	event := validDeadLetterEvent()
	event.OriginalValue = "   "

	err := event.Validate()
	if err == nil {
		t.Fatal("expected error for blank original_value, got nil")
	}
	if !strings.Contains(err.Error(), "original_value") {
		t.Fatalf("expected original_value error, got: %v", err)
	}
}

func TestDeadLetterEventValidateFailsForBlankSourceTopic(t *testing.T) {
	event := validDeadLetterEvent()
	event.SourceTopic = "   "

	err := event.Validate()
	if err == nil {
		t.Fatal("expected error for blank source_topic, got nil")
	}
	if !strings.Contains(err.Error(), "source_topic") {
		t.Fatalf("expected source_topic error, got: %v", err)
	}
}

func TestDeadLetterEventValidateFailsForBlankWorker(t *testing.T) {
	event := validDeadLetterEvent()
	event.Worker = "   "

	err := event.Validate()
	if err == nil {
		t.Fatal("expected error for blank worker, got nil")
	}
	if !strings.Contains(err.Error(), "worker") {
		t.Fatalf("expected worker error, got: %v", err)
	}
}

func TestDeadLetterEventValidateAllowsBlankOriginalKey(t *testing.T) {
	event := validDeadLetterEvent()
	event.OriginalKey = ""

	err := event.Validate()
	if err != nil {
		t.Fatalf("expected blank original_key to be allowed, got error: %v", err)
	}
}

func TestDeadLetterEventToJSONReturnsExpectedFields(t *testing.T) {
	event := validDeadLetterEvent()

	jsonText, err := event.ToJSON()
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	var decoded map[string]string
	if err := json.Unmarshal([]byte(jsonText), &decoded); err != nil {
		t.Fatalf("expected valid JSON, got parse error: %v", err)
	}

	if decoded["reason"] != event.Reason {
		t.Fatalf("expected reason %q, got %q", event.Reason, decoded["reason"])
	}
	if decoded["original_key"] != event.OriginalKey {
		t.Fatalf("expected original_key %q, got %q", event.OriginalKey, decoded["original_key"])
	}
	if decoded["original_value"] != event.OriginalValue {
		t.Fatalf("expected original_value %q, got %q", event.OriginalValue, decoded["original_value"])
	}
	if decoded["source_topic"] != event.SourceTopic {
		t.Fatalf("expected source_topic %q, got %q", event.SourceTopic, decoded["source_topic"])
	}
	if decoded["worker"] != event.Worker {
		t.Fatalf("expected worker %q, got %q", event.Worker, decoded["worker"])
	}
}
