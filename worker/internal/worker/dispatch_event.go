// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the JSON event shape the Go worker will consume from Kafka.
package worker

import (
	"encoding/json"
	"fmt"
	"strings"
)

// DispatchEvent is the event contract published by the Python control plane.
//
// JSON tags must match the Python DispatchEvent field names exactly so the
// worker can decode messages from `kernelq.jobs.dispatch` reliably.
type DispatchEvent struct {
	EventType string            `json:"event_type"`
	JobID     string            `json:"job_id"`
	TenantID  string            `json:"tenant_id"`
	Priority  int               `json:"priority"`
	State     string            `json:"state"`
	Payload   map[string]string `json:"payload"`
}

// ValidateDispatchEvent checks that a decoded dispatch event has the minimum
// required shape and values before the worker tries to execute anything.
func ValidateDispatchEvent(event DispatchEvent) error {
	// For now, workers only accept one event kind.
	if event.EventType != "job.dispatch" {
		return fmt.Errorf("event_type must be %q, got %q", "job.dispatch", event.EventType)
	}

	if strings.TrimSpace(event.JobID) == "" {
		return fmt.Errorf("job_id must not be blank")
	}

	if strings.TrimSpace(event.TenantID) == "" {
		return fmt.Errorf("tenant_id must not be blank")
	}

	if event.Priority < 0 {
		return fmt.Errorf("priority must be >= 0, got %d", event.Priority)
	}

	// The scheduler currently publishes events in dispatched state only.
	if event.State != "dispatched" {
		return fmt.Errorf("state must be %q, got %q", "dispatched", event.State)
	}

	// Payload may be nil for now.
	return nil
}

// ParseDispatchEvent decodes Kafka JSON bytes into a DispatchEvent and validates
// the result before returning it to worker code.
func ParseDispatchEvent(data []byte) (DispatchEvent, error) {
	var event DispatchEvent

	// Step 1: parse JSON into the struct shape.
	if err := json.Unmarshal(data, &event); err != nil {
		return DispatchEvent{}, err
	}

	// Step 2: enforce required values and allowed states.
	if err := ValidateDispatchEvent(event); err != nil {
		return DispatchEvent{}, err
	}

	return event, nil
}
