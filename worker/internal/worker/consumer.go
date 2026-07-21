// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the worker-side message-processing boundary. A real Kafka
// client will eventually read bytes from the broker and pass them here as
// Message values. Today we only process messages in memory (tests, fakes).
package worker

import (
	"context"
	"fmt"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/codes"
)

// Message is one record the worker received from a broker (or a test fake).
//
// Key is usually the job_id (Kafka message key from the Python producer).
// Value is the raw JSON body (DispatchEvent bytes).
// Headers carry W3C trace context (and other non-payload metadata).
type Message struct {
	Key       string
	Value     []byte
	Headers   []kafka.Header
	Topic     string
	Partition int32
	Offset    int64
}

// DispatchHandler runs business logic for one validated dispatch event.
//
// A future execution pipeline will implement this interface. Tests can use a
// small fake handler that records events or returns errors on purpose.
// Context carries deadlines and OpenTelemetry span context.
type DispatchHandler interface {
	Handle(ctx context.Context, event DispatchEvent) (ExecutionResult, error)
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
//  1. Extract remote trace context from Kafka headers (safe if missing).
//  2. Start kafka.process span.
//  3. Parse and validate JSON (ParseDispatchEvent).
//  4. Call Handler.Handle with the process span context.
func (runner ConsumerRunner) ProcessMessage(ctx context.Context, message Message) error {
	if ctx == nil {
		ctx = context.Background()
	}
	parentCtx := telemetry.ExtractKafkaContext(ctx, message.Headers)
	topic := message.Topic
	if topic == "" {
		topic = "unknown"
	}
	ctx, span := telemetry.StartKafkaProcessSpan(parentCtx, topic, message.Partition, message.Offset)
	defer span.End()

	event, err := ParseDispatchEvent(message.Value)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if runner.Handler == nil {
		err := fmt.Errorf("dispatch handler is not configured")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	if _, err := runner.Handler.Handle(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}
