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
//	DispatchEvent → Task → Executor.Execute
type DispatchEventHandler struct {
	Executor Executor
}

// Handle converts one dispatch event into a Task and runs it.
//
// DispatchEvent was already validated at parse time (ParseDispatchEvent).
// We validate Task again as a safety check before execution.
func (handler DispatchEventHandler) Handle(event DispatchEvent) error {
	if handler.Executor == nil {
		return fmt.Errorf("executor is not configured")
	}

	// Map the cross-language event contract onto the worker Task model.
	task := Task{
		JobID:    event.JobID,
		TenantID: event.TenantID,
		Priority: event.Priority,
		Payload:  event.Payload,
	}

	if err := ValidateTask(task); err != nil {
		return err
	}

	if err := handler.Executor.Execute(task); err != nil {
		return err
	}

	return nil
}
