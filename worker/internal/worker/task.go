// Package worker holds types and logic for the KernelQ worker plane.
//
// Workers eventually consume dispatch events from Kafka and run jobs.
// This file defines a simple Task model—the unit of work a worker executes.
package worker

import (
	"fmt"
	"strings"
)

// Task is one runnable job after the control plane has scheduled it.
//
// Fields mirror the important parts of a DispatchEvent from the Python
// control plane (job_id, tenant_id, priority, payload). Payload values are
// strings for now to keep the first version simple.
type Task struct {
	JobID    string
	TenantID string
	Priority int
	// Payload holds job input data. Nil is allowed until we wire richer types.
	Payload map[string]string
}

// ValidateTask checks that a Task has the minimum fields required before
// a worker tries to run it. Returns nil when the task is valid.
func ValidateTask(task Task) error {
	if strings.TrimSpace(task.JobID) == "" {
		return fmt.Errorf("job id must not be blank")
	}

	if strings.TrimSpace(task.TenantID) == "" {
		return fmt.Errorf("tenant id must not be blank")
	}

	if task.Priority < 0 {
		return fmt.Errorf("priority must be >= 0, got %d", task.Priority)
	}

	// Payload may be nil or empty for now—no validation required yet.
	return nil
}
