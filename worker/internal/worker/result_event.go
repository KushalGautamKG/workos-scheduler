// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the JSON event shape workers will publish when a job
// attempt finishes. Result events travel on kernelq.jobs.results so the
// Python control plane can update Postgres and drive retry policy.
package worker

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ResultTopic is the Kafka topic where workers publish execution outcomes.
const ResultTopic = "kernelq.jobs.results"

// workerResultEventType is the only event_type value allowed on ResultTopic.
const workerResultEventType = "job.result"

// WorkerResultEvent is the on-wire contract for reporting job outcomes.
//
// Workers produce these after execution; the control plane consumes them to
// move jobs toward succeeded, failed/retry_scheduled, or dead_lettered.
type WorkerResultEvent struct {
	EventType string          `json:"event_type"`
	JobID     string          `json:"job_id"`
	Status    ExecutionStatus `json:"status"`
	Message   string          `json:"message"`
	Worker    string          `json:"worker"`
}

// NewWorkerResultEvent builds a result event from a job id, execution outcome,
// and worker identity. It does not validate—call Validate() or ToJSON() before
// publishing.
func NewWorkerResultEvent(jobID string, result ExecutionResult, worker string) WorkerResultEvent {
	return WorkerResultEvent{
		EventType: workerResultEventType,
		JobID:     jobID,
		Status:    result.Status,
		Message:   result.Message,
		Worker:    worker,
	}
}

// Validate checks required fields before we publish to kernelq.jobs.results.
func (e WorkerResultEvent) Validate() error {
	if e.EventType != workerResultEventType {
		return fmt.Errorf("event_type must be %q, got %q", workerResultEventType, e.EventType)
	}

	if strings.TrimSpace(e.JobID) == "" {
		return fmt.Errorf("job_id must not be blank")
	}

	// Reuse ExecutionResult rules: status must be a known constant; message may be blank.
	outcome := ExecutionResult{Status: e.Status, Message: e.Message}
	if err := outcome.Validate(); err != nil {
		return err
	}

	if strings.TrimSpace(e.Worker) == "" {
		return fmt.Errorf("worker must not be blank")
	}

	return nil
}

// ToJSON encodes the event as a JSON string after validation.
func (e WorkerResultEvent) ToJSON() (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}

	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
