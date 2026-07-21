// Package worker holds types and logic for the KernelQ worker plane.
//
// This file defines the result publishing boundary. After a worker executes
// a job, it can publish a WorkerResultEvent so the control plane learns the
// outcome. Real Kafka publishing comes later; tests use RecordingResultProducer.
package worker

import (
	"context"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ResultProducer publishes validated worker result events.
//
// Production code uses a Kafka-backed implementation (kernelq.jobs.results).
// Tests use RecordingResultProducer to capture events in memory—no broker needed.
// Context carries the active execution span for kafka.publish child spans.
type ResultProducer interface {
	PublishResult(ctx context.Context, event WorkerResultEvent) error
}

// RecordingResultProducer is an in-memory ResultProducer for tests and demos.
//
// It validates each event and appends it to Published. Nothing is sent to Kafka.
type RecordingResultProducer struct {
	Published []WorkerResultEvent
	Headers   [][]kafka.Header // injected headers per publish (for tests)
}

// PublishResult validates the event and stores a copy in Published.
// Starts a kafka.publish child span and injects W3C headers (mirrors Kafka path).
func (p *RecordingResultProducer) PublishResult(ctx context.Context, event WorkerResultEvent) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, span := telemetry.StartKafkaPublishSpan(ctx, ResultTopic, "publish")
	defer span.End()

	if err := event.Validate(); err != nil {
		telemetry.RecordSpanError(span, err)
		return err
	}

	headers := []kafka.Header{}
	telemetry.InjectKafkaContext(ctx, &headers)
	p.Headers = append(p.Headers, headers)
	p.Published = append(p.Published, event)
	return nil
}
