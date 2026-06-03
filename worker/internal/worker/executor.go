// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the execution boundary: what it means to "run" a Task after
// a dispatch event has been parsed and validated.
package worker

// Executor is the boundary for actual task execution.
//
// ConsumerRunner and DispatchHandler get messages off the wire and into a
// validated DispatchEvent. An Executor is what turns a Task into "work done"
// (or a controlled failure).
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
	Execute(task Task) error
}
