package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	// TracerName is the instrumentation scope for worker spans.
	TracerName = "kernelq.worker"

	// SpanWorkerExecute is the unit-of-work span around job execution.
	SpanWorkerExecute = "worker.execute"
)

// StartExecutionSpan starts a worker.execute span with job.id and job.attempt.
// Callers must End the span (typically via defer). Do not attach payloads.
func StartExecutionSpan(ctx context.Context, jobID string, attempt int) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	tracer := otel.Tracer(TracerName)
	return tracer.Start(
		ctx,
		SpanWorkerExecute,
		trace.WithAttributes(
			JobID(jobID),
			Attempt(attempt),
		),
	)
}

// RecordExecutionSuccess marks a successful execution on the span.
func RecordExecutionSuccess(span trace.Span) {
	if span == nil {
		return
	}
	span.SetAttributes(
		ExecutionStatus(ExecutionStatusSuccess),
		DuplicateSkipped(false),
	)
	span.SetStatus(codes.Ok, "")
}

// RecordExecutionDuplicate marks a duplicate-skipped execution on the span.
func RecordExecutionDuplicate(span trace.Span) {
	if span == nil {
		return
	}
	span.SetAttributes(
		ExecutionStatus(ExecutionStatusDuplicate),
		DuplicateSkipped(true),
	)
	span.SetStatus(codes.Ok, "duplicate execution skipped")
}

// RecordExecutionFailure marks a failed execution and records the error.
func RecordExecutionFailure(span trace.Span, err error) {
	if span == nil {
		return
	}
	span.SetAttributes(
		ExecutionStatus(ExecutionStatusFailed),
		DuplicateSkipped(false),
	)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Error, "execution failed")
	}
}
