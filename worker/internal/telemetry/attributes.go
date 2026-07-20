package telemetry

import "go.opentelemetry.io/otel/attribute"

// Stable attribute keys for worker execution spans (Day 120).
const (
	AttrJobID             = "job.id"
	AttrJobAttempt        = "job.attempt"
	AttrExecutionStatus   = "execution.status"
	AttrDuplicateSkipped  = "duplicate_skipped"

	ExecutionStatusSuccess   = "success"
	ExecutionStatusDuplicate = "duplicate"
	ExecutionStatusFailed    = "failed"
)

// JobID returns the job.id attribute.
func JobID(jobID string) attribute.KeyValue {
	return attribute.String(AttrJobID, jobID)
}

// Attempt returns the job.attempt attribute.
func Attempt(attempt int) attribute.KeyValue {
	return attribute.Int(AttrJobAttempt, attempt)
}

// ExecutionStatus returns the execution.status attribute.
func ExecutionStatus(status string) attribute.KeyValue {
	return attribute.String(AttrExecutionStatus, status)
}

// DuplicateSkipped returns the duplicate_skipped attribute.
func DuplicateSkipped(skipped bool) attribute.KeyValue {
	return attribute.Bool(AttrDuplicateSkipped, skipped)
}
