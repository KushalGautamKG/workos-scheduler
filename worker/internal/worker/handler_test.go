package worker

import (
	"errors"
	"strings"
	"testing"
)

// fakeExecutor records the Task passed to Execute and can return a test error.
type fakeExecutor struct {
	received Task
	err      error
}

func (executor *fakeExecutor) Execute(task Task) error {
	executor.received = task
	return executor.err
}

func validDispatchEventForHandler() DispatchEvent {
	return DispatchEvent{
		EventType: "job.dispatch",
		JobID:     "job-123",
		TenantID:  "tenant-a",
		Priority:  5,
		State:     "dispatched",
		Payload:   map[string]string{"kind": "day35-smoke"},
	}
}

func TestHandleConvertsDispatchEventToTaskAndCallsExecutor(t *testing.T) {
	executor := &fakeExecutor{}
	handler := DispatchEventHandler{Executor: executor}
	event := validDispatchEventForHandler()

	err := handler.Handle(event)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	if executor.received.JobID != event.JobID {
		t.Fatalf("expected job id %q, got %q", event.JobID, executor.received.JobID)
	}
	if executor.received.TenantID != event.TenantID {
		t.Fatalf("expected tenant id %q, got %q", event.TenantID, executor.received.TenantID)
	}
	if executor.received.Priority != event.Priority {
		t.Fatalf("expected priority %d, got %d", event.Priority, executor.received.Priority)
	}
	if executor.received.Payload["kind"] != "day35-smoke" {
		t.Fatalf("expected payload kind day35-smoke, got %v", executor.received.Payload)
	}
}

func TestHandleReturnsErrorIfExecutorIsNil(t *testing.T) {
	handler := DispatchEventHandler{Executor: nil}

	err := handler.Handle(validDispatchEventForHandler())
	if err == nil {
		t.Fatal("expected error when executor is nil, got nil")
	}
	if !strings.Contains(err.Error(), "executor") {
		t.Fatalf("expected executor error, got: %v", err)
	}
}

func TestHandleReturnsValidationErrorForInvalidTask(t *testing.T) {
	executor := &fakeExecutor{}
	handler := DispatchEventHandler{Executor: executor}

	event := validDispatchEventForHandler()
	event.JobID = "   "

	err := handler.Handle(event)
	if err == nil {
		t.Fatal("expected validation error for blank job id, got nil")
	}
	if !strings.Contains(err.Error(), "job id") {
		t.Fatalf("expected job id validation error, got: %v", err)
	}
}

func TestHandleReturnsExecutorError(t *testing.T) {
	expectedErr := errors.New("simulated execution failure")
	executor := &fakeExecutor{err: expectedErr}
	handler := DispatchEventHandler{Executor: executor}

	err := handler.Handle(validDispatchEventForHandler())
	if err == nil {
		t.Fatal("expected executor error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}
