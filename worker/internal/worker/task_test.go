package worker

import (
	"strings"
	"testing"
)

// validTask returns a Task that should pass ValidateTask.
func validTask() Task {
	return Task{
		JobID:    "job-123",
		TenantID: "tenant-a",
		Priority: 10,
		Payload:  map[string]string{"kind": "billing-export"},
	}
}

func TestValidateTaskAcceptsValidTask(t *testing.T) {
	task := validTask()

	err := ValidateTask(task)
	if err != nil {
		t.Fatalf("expected valid task, got error: %v", err)
	}
}

func TestValidateTaskAcceptsNilPayload(t *testing.T) {
	task := validTask()
	task.Payload = nil

	err := ValidateTask(task)
	if err != nil {
		t.Fatalf("expected nil payload to be allowed, got error: %v", err)
	}
}

func TestValidateTaskRejectsBlankJobID(t *testing.T) {
	task := validTask()
	task.JobID = "   "

	err := ValidateTask(task)
	if err == nil {
		t.Fatal("expected error for blank job id, got nil")
	}
	if !strings.Contains(err.Error(), "job id") {
		t.Fatalf("expected job id error, got: %v", err)
	}
}

func TestValidateTaskRejectsBlankTenantID(t *testing.T) {
	task := validTask()
	task.TenantID = ""

	err := ValidateTask(task)
	if err == nil {
		t.Fatal("expected error for blank tenant id, got nil")
	}
	if !strings.Contains(err.Error(), "tenant id") {
		t.Fatalf("expected tenant id error, got: %v", err)
	}
}

func TestValidateTaskRejectsNegativePriority(t *testing.T) {
	task := validTask()
	task.Priority = -1

	err := ValidateTask(task)
	if err == nil {
		t.Fatal("expected error for negative priority, got nil")
	}
	if !strings.Contains(err.Error(), "priority") {
		t.Fatalf("expected priority error, got: %v", err)
	}
}
