package worker

import (
	"strings"
	"testing"
)

// validDispatchJSON returns one well-formed, valid dispatch event payload.
func validDispatchJSON() []byte {
	return []byte(`{
		"event_type":"job.dispatch",
		"job_id":"job-123",
		"tenant_id":"tenant-a",
		"priority":5,
		"state":"dispatched",
		"payload":{"kind":"day32-smoke"}
	}`)
}

func TestParseDispatchEventSucceedsForValidJSON(t *testing.T) {
	event, err := ParseDispatchEvent(validDispatchJSON())
	if err != nil {
		t.Fatalf("expected valid dispatch event, got error: %v", err)
	}

	if event.EventType != "job.dispatch" {
		t.Fatalf("expected event_type job.dispatch, got %q", event.EventType)
	}
	if event.JobID != "job-123" {
		t.Fatalf("expected job_id job-123, got %q", event.JobID)
	}
}

func TestParseDispatchEventFailsForInvalidEventType(t *testing.T) {
	data := []byte(`{
		"event_type":"job.retry",
		"job_id":"job-123",
		"tenant_id":"tenant-a",
		"priority":5,
		"state":"dispatched"
	}`)

	_, err := ParseDispatchEvent(data)
	if err == nil {
		t.Fatal("expected error for invalid event_type, got nil")
	}
}

func TestParseDispatchEventFailsForBlankJobID(t *testing.T) {
	data := []byte(`{
		"event_type":"job.dispatch",
		"job_id":"  ",
		"tenant_id":"tenant-a",
		"priority":5,
		"state":"dispatched"
	}`)

	_, err := ParseDispatchEvent(data)
	if err == nil {
		t.Fatal("expected error for blank job_id, got nil")
	}
	if !strings.Contains(err.Error(), "job_id") {
		t.Fatalf("expected job_id error, got: %v", err)
	}
}

func TestParseDispatchEventFailsForBlankTenantID(t *testing.T) {
	data := []byte(`{
		"event_type":"job.dispatch",
		"job_id":"job-123",
		"tenant_id":" ",
		"priority":5,
		"state":"dispatched"
	}`)

	_, err := ParseDispatchEvent(data)
	if err == nil {
		t.Fatal("expected error for blank tenant_id, got nil")
	}
	if !strings.Contains(err.Error(), "tenant_id") {
		t.Fatalf("expected tenant_id error, got: %v", err)
	}
}

func TestParseDispatchEventFailsForNegativePriority(t *testing.T) {
	data := []byte(`{
		"event_type":"job.dispatch",
		"job_id":"job-123",
		"tenant_id":"tenant-a",
		"priority":-1,
		"state":"dispatched"
	}`)

	_, err := ParseDispatchEvent(data)
	if err == nil {
		t.Fatal("expected error for negative priority, got nil")
	}
	if !strings.Contains(err.Error(), "priority") {
		t.Fatalf("expected priority error, got: %v", err)
	}
}

func TestParseDispatchEventFailsForInvalidState(t *testing.T) {
	data := []byte(`{
		"event_type":"job.dispatch",
		"job_id":"job-123",
		"tenant_id":"tenant-a",
		"priority":5,
		"state":"queued"
	}`)

	_, err := ParseDispatchEvent(data)
	if err == nil {
		t.Fatal("expected error for invalid state, got nil")
	}
	if !strings.Contains(err.Error(), "state") {
		t.Fatalf("expected state error, got: %v", err)
	}
}

func TestParseDispatchEventFailsForMalformedJSON(t *testing.T) {
	data := []byte(`{"event_type":"job.dispatch",`)

	_, err := ParseDispatchEvent(data)
	if err == nil {
		t.Fatal("expected parse error for malformed JSON, got nil")
	}
}
