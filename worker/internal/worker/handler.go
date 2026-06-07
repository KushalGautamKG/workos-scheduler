// Package worker holds types and logic for the KernelQ worker plane.
//
// This file connects validated dispatch events to task execution. ConsumerRunner
// parses Kafka JSON; DispatchEventHandler turns a DispatchEvent into a Task
// and calls an Executor.
package worker

import "fmt"

// DispatchEventHandler implements DispatchHandler by mapping events to Tasks
// and delegating to an Executor.
//
// This sits between "message is valid" and "job actually runs":
//
//	DispatchEvent → Task → Executor.Execute → ExecutionResult
type DispatchEventHandler struct {
	Executor Executor
}

// Handle converts one dispatch event into a Task and runs it.
//
// Returns:
//   - ExecutionResult — structured job outcome when execution completes normally
//     (success, retryable failure, or terminal failure)
//   - error — configuration, validation, infrastructure, or invalid-outcome errors
//
// DispatchEvent was already validated at parse time (ParseDispatchEvent).
// We validate Task again as a safety check before execution.
func (handler DispatchEventHandler) Handle(event DispatchEvent) (ExecutionResult, error) {
	// Step 1: ensure an executor is wired.
	if handler.Executor == nil {
		return ExecutionResult{}, fmt.Errorf("executor is not configured")
	}

	// Step 2: map the cross-language event contract onto the worker Task model.
	task := Task{
		JobID:    event.JobID,
		TenantID: event.TenantID,
		Priority: event.Priority,
		Payload:  event.Payload,
	}

	// Step 3: validate the Task before we attempt execution.
	if err := ValidateTask(task); err != nil {
		return ExecutionResult{}, err
	}

	// Step 4: run the task through the execution boundary.
	result, err := handler.Executor.Execute(task)
	if err != nil {
		// Infrastructure failure (for example Postgres unreachable). The executor
		// could not report a trustworthy job outcome—return the error and let
		// callers treat this separately from retry/terminal business failures.
		return ExecutionResult{}, err
	}

	// Step 5: validate the outcome before we pass it upstream.
	// Executors must return one of the known ExecutionStatus constants.
	if err := result.Validate(); err != nil {
		return ExecutionResult{}, err
	}

	return result, nil
}
