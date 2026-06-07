// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines how a worker reports the outcome of running a job.
// A plain Go error only says "something failed." ExecutionResult says
// what kind of failure it was so callers can choose retry, dead-letter,
// or success handling without guessing from error strings.
package worker

import "fmt"

// ExecutionStatus classifies the outcome of one job execution attempt.
//
// These values align with KernelQ's job lifecycle in Postgres:
//   - ExecutionSucceeded        → job can move to succeeded
//   - ExecutionRetryableFailure → job may move to failed → retry_scheduled
//   - ExecutionTerminalFailure  → job should move to dead_lettered (or DLQ)
type ExecutionStatus string

const (
	// ExecutionSucceeded means the job finished successfully.
	ExecutionSucceeded ExecutionStatus = "succeeded"

	// ExecutionRetryableFailure means the job failed but may succeed on a
	// later attempt (for example a transient dependency timeout).
	ExecutionRetryableFailure ExecutionStatus = "retryable_failure"

	// ExecutionTerminalFailure means the job failed and should not be
	// retried automatically (for example invalid payload or max retries hit).
	ExecutionTerminalFailure ExecutionStatus = "terminal_failure"
)

// executionStatuses lists every valid ExecutionStatus constant.
// Validate() compares against this slice so we reject typos and unknown values.
var executionStatuses = []ExecutionStatus{
	ExecutionSucceeded,
	ExecutionRetryableFailure,
	ExecutionTerminalFailure,
}

// ExecutionResult is the structured outcome returned by job execution logic.
//
// Status tells callers what to do next. Message is an optional human-readable
// detail for logs, metrics, and Postgres/DLQ records—it may be blank.
type ExecutionResult struct {
	Status  ExecutionStatus
	Message string
}

// Validate checks that Status is one of the defined constants.
//
// Message is intentionally not required: a blank message is valid for all
// statuses (for example SuccessResult() with no extra detail).
func (r ExecutionResult) Validate() error {
	for _, allowed := range executionStatuses {
		if r.Status == allowed {
			return nil
		}
	}

	return fmt.Errorf("status must be one of %q, %q, %q, got %q",
		ExecutionSucceeded,
		ExecutionRetryableFailure,
		ExecutionTerminalFailure,
		r.Status,
	)
}

// SuccessResult returns a valid result for a job that completed successfully.
func SuccessResult() ExecutionResult {
	return ExecutionResult{
		Status: ExecutionSucceeded,
	}
}

// RetryableFailureResult returns a valid result for a transient failure.
//
// message may be blank; when set, it explains why the attempt failed.
func RetryableFailureResult(message string) ExecutionResult {
	return ExecutionResult{
		Status:  ExecutionRetryableFailure,
		Message: message,
	}
}

// TerminalFailureResult returns a valid result for a permanent failure.
//
// message may be blank; when set, it explains why retries should stop.
func TerminalFailureResult(message string) ExecutionResult {
	return ExecutionResult{
		Status:  ExecutionTerminalFailure,
		Message: message,
	}
}
