// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the execution boundary: what it means to "run" a Task after
// a dispatch event has been parsed and validated.
package worker

// Executor is the boundary for actual task execution.
//
// ConsumerRunner and DispatchEventHandler get messages off the wire and into a
// validated DispatchEvent. An Executor is what turns a Task into "work done"
// (or a controlled failure).
//
// Execute returns two values on purpose:
//   - ExecutionResult describes the job outcome (success, retryable failure,
//     or terminal failure). Callers use Status—not the error—to decide retry
//     vs dead-letter vs mark succeeded in Postgres.
//   - error represents execution infrastructure failures (for example the
//     executor could not reach Postgres or crashed before reporting an outcome).
//     When err is non-nil, the outcome in ExecutionResult should be ignored.
//
// Later implementations will add:
//   - concurrency limits (bounded worker pool)
//   - timeouts (do not run forever)
//   - cancellation (shutdown, lease expiry)
//   - metrics (latency, success/failure counts)
//
// Today we only define the interface so tests and future handlers can depend
// on a stable contract without wiring real job logic yet.
type Executor interface {
	Execute(task Task) (ExecutionResult, error)
}
