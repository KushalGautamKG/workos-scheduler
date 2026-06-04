// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the dead-letter queue (DLQ) event shape and producer
// boundary. When a dispatch message cannot be processed, the worker can
// publish a DeadLetterEvent to kernelq.jobs.dlq for inspection and replay.
package worker

import (
	"encoding/json"
	"fmt"
	"strings"
)

// DLQTopic is the Kafka topic for poison or permanently failed messages.
const DLQTopic = "kernelq.jobs.dlq"

// DeadLetterEvent describes one message the worker could not process on the
// dispatch topic. It preserves the original key/value plus metadata so
// operators can debug and replay later.
type DeadLetterEvent struct {
	Reason        string `json:"reason"`
	OriginalKey   string `json:"original_key"`
	OriginalValue string `json:"original_value"`
	SourceTopic   string `json:"source_topic"`
	Worker        string `json:"worker"`
}

// Validate checks that a dead-letter event has the minimum required fields
// before we publish it to the DLQ topic.
func (e DeadLetterEvent) Validate() error {
	if strings.TrimSpace(e.Reason) == "" {
		return fmt.Errorf("reason must not be blank")
	}

	if strings.TrimSpace(e.OriginalValue) == "" {
		return fmt.Errorf("original_value must not be blank")
	}

	if strings.TrimSpace(e.SourceTopic) == "" {
		return fmt.Errorf("source_topic must not be blank")
	}

	if strings.TrimSpace(e.Worker) == "" {
		return fmt.Errorf("worker must not be blank")
	}

	// OriginalKey may be blank (some producers omit Kafka keys).
	return nil
}

// ToJSON encodes the event as a JSON string after validation.
func (e DeadLetterEvent) ToJSON() (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}

	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

// DeadLetterProducer publishes validated dead-letter events.
//
// Real Kafka publishing comes later. Tests can use a fake implementation
// that records events without connecting to a broker.
type DeadLetterProducer interface {
	PublishDeadLetter(event DeadLetterEvent) error
}
