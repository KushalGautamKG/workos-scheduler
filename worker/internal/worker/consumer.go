// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the worker-side message-processing boundary. A real Kafka
// client will eventually read bytes from the broker and pass them here as
// Message values. Today we only process messages in memory (tests, fakes).
package worker

import "fmt"

// Message is one record the worker received from a broker (or a test fake).
//
// Key is usually the job_id (Kafka message key from the Python producer).
// Value is the raw JSON body (DispatchEvent bytes).
type Message struct {
	Key   string
	Value []byte
}

// DispatchHandler runs business logic for one validated dispatch event.
//
// A future execution pipeline will implement this interface. Tests can use a
// small fake handler that records events or returns errors on purpose.
type DispatchHandler interface {
	Handle(event DispatchEvent) (ExecutionResult, error)
}

// ConsumerRunner connects "message bytes in" to "handler logic out".
//
// It does not talk to Kafka directly—that keeps broker SDK code separate from
// parsing, validation, and job handling.
type ConsumerRunner struct {
	Handler DispatchHandler
}

// ProcessMessage is the single entry point for one dispatch message.
//
// Steps:
//  1. Parse and validate JSON (ParseDispatchEvent).
//  2. Ensure a handler is configured.
//  3. Call Handler.Handle with the validated event.
func (runner ConsumerRunner) ProcessMessage(message Message) error {
	// Turn broker bytes into a typed, validated DispatchEvent.
	event, err := ParseDispatchEvent(message.Value)
	if err != nil {
		return err
	}

	if runner.Handler == nil {
		return fmt.Errorf("dispatch handler is not configured")
	}

	// Hand off to execution logic (real or test fake).
	if _, err := runner.Handler.Handle(event); err != nil {
		return err
	}

	return nil
}
