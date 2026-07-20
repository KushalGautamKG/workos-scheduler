// Package worker holds types and logic for the KernelQ worker plane.
//
// This file connects validated dispatch events to task execution. ConsumerRunner
// parses Kafka JSON; DispatchEventHandler turns a DispatchEvent into a Task
// and calls an Executor.
package worker

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
)

// DefaultExecutionIdempotencyTTL is used when IdempotencyStore is set and
// IdempotencyTTL is zero (24 hours, aligned with Python/result dedupe).
const DefaultExecutionIdempotencyTTL = 24 * time.Hour

// DispatchEventHandler implements DispatchHandler by mapping events to Tasks
// and delegating to an Executor.
//
// This sits between "message is valid" and "job actually runs":
//
//	DispatchEvent → (optional TryClaim) → Task → Executor.Execute → ExecutionResult
//
// Use a pointer so idempotency counters stay shared across pool workers.
type DispatchEventHandler struct {
	Executor         Executor
	ResultProducer   ResultProducer // optional: publish outcomes to kernelq.jobs.results
	WorkerName       string         // optional: worker identity on result events
	IdempotencyStore IdempotencyStore
	IdempotencyTTL   time.Duration // 0 ⇒ DefaultExecutionIdempotencyTTL when store set

	duplicateExecutions atomic.Int64
	idempotencyErrors   atomic.Int64
}

// DuplicateExecutions returns how many dispatch replays were skipped.
func (handler *DispatchEventHandler) DuplicateExecutions() int64 {
	return handler.duplicateExecutions.Load()
}

// IdempotencyErrors returns how many TryClaim store failures occurred.
func (handler *DispatchEventHandler) IdempotencyErrors() int64 {
	return handler.idempotencyErrors.Load()
}

// Handle converts one dispatch event into a Task and runs it.
//
// Returns:
//   - ExecutionResult — structured job outcome when execution completes normally
//     (success, retryable failure, terminal failure, or duplicate skipped)
//   - error — configuration, validation, infrastructure, publish, or invalid-outcome errors
//
// DispatchEvent was already validated at parse time (ParseDispatchEvent).
// We validate Task again as a safety check before execution.
//
// Day 120: wraps the unit of work in a worker.execute span (no payload attrs).
func (handler *DispatchEventHandler) Handle(
	ctx context.Context,
	event DispatchEvent,
) (ExecutionResult, error) {
	ctx, span := telemetry.StartExecutionSpan(ctx, event.JobID, event.Attempt)
	defer span.End()

	result, err := handler.handle(ctx, event)
	if err != nil {
		telemetry.RecordExecutionFailure(span, err)
		return result, err
	}

	switch result.Status {
	case ExecutionDuplicateSkipped:
		telemetry.RecordExecutionDuplicate(span)
	case ExecutionSucceeded:
		telemetry.RecordExecutionSuccess(span)
	case ExecutionRetryableFailure, ExecutionTerminalFailure:
		telemetry.RecordExecutionFailure(span, fmt.Errorf("%s", result.Message))
	default:
		telemetry.RecordExecutionFailure(span, fmt.Errorf("unknown execution status %q", result.Status))
	}

	return result, nil
}

func (handler *DispatchEventHandler) handle(
	ctx context.Context,
	event DispatchEvent,
) (ExecutionResult, error) {
	_ = ctx

	// Step 1: ensure an executor is wired.
	if handler.Executor == nil {
		return ExecutionResult{}, fmt.Errorf("executor is not configured")
	}

	// Step 2: optional execution idempotency claim before Execute.
	if handler.IdempotencyStore != nil {
		key, err := ExecutionIdempotencyKey(event.JobID, event.Attempt)
		if err != nil {
			return ExecutionResult{}, err
		}

		ttl := handler.IdempotencyTTL
		if ttl <= 0 {
			ttl = DefaultExecutionIdempotencyTTL
		}

		claimed, err := handler.IdempotencyStore.TryClaim(key, ttl)
		if err != nil {
			handler.idempotencyErrors.Add(1)
			return ExecutionResult{}, fmt.Errorf("idempotency claim failed: %w", err)
		}
		if !claimed {
			handler.duplicateExecutions.Add(1)
			fmt.Printf(
				"event=duplicate_worker_execution job_id=%s attempt=%d\n",
				event.JobID,
				event.Attempt,
			)
			// Duplicate skip is not a failure and must not go to DLQ.
			// Do not publish to results (Python does not accept duplicate_skipped yet).
			return DuplicateSkippedResult(), nil
		}
	}

	// Step 3: map the cross-language event contract onto the worker Task model.
	task := Task{
		JobID:    event.JobID,
		TenantID: event.TenantID,
		Priority: event.Priority,
		Payload:  event.Payload,
	}

	// Step 4: validate the Task before we attempt execution.
	if err := ValidateTask(task); err != nil {
		return ExecutionResult{}, err
	}

	// Step 5: run the task through the execution boundary.
	result, err := handler.Executor.Execute(task)
	if err != nil {
		// Infrastructure failure (for example Postgres unreachable). The executor
		// could not report a trustworthy job outcome—return the error and let
		// callers treat this separately from retry/terminal business failures.
		return ExecutionResult{}, err
	}

	// Step 6: validate the outcome before we pass it upstream.
	// Executors must return one of the known ExecutionStatus constants.
	if err := result.Validate(); err != nil {
		return ExecutionResult{}, err
	}

	// Step 7: optionally publish a WorkerResultEvent for the control plane.
	// Skip duplicate_skipped (should not reach here) and nil producers.
	if handler.ResultProducer != nil && result.Status != ExecutionDuplicateSkipped {
		workerName := handler.WorkerName
		if strings.TrimSpace(workerName) == "" {
			workerName = workerIdentity
		}

		resultEvent := NewWorkerResultEvent(event.JobID, result, workerName)
		if err := handler.ResultProducer.PublishResult(resultEvent); err != nil {
			// Execution succeeded from the executor's perspective, but we could
			// not hand the outcome to Kafka—return the result plus the error.
			return result, err
		}
	}

	return result, nil
}
