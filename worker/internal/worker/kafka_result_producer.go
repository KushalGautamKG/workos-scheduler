// Package worker holds types and logic for the KernelQ worker plane.
//
// This file implements ResultProducer with a real confluent-kafka-go client.
// After execution, workers can publish WorkerResultEvent JSON to
// kernelq.jobs.results for the control plane to consume.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/logging"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// resultFlushTimeoutMs is how long PublishResult waits for the broker to
// accept the message before returning a delivery timeout error.
const resultFlushTimeoutMs = 5000

// KafkaResultProducer publishes WorkerResultEvent JSON to a Kafka results topic.
//
// It reuses KafkaProducerClient from kafka_dlq_producer.go so tests can inject
// the same fake client without connecting to a broker.
type KafkaResultProducer struct {
	Producer KafkaProducerClient
	Topic    string
	Logger   *slog.Logger // optional; nil ⇒ no structured publish logs
}

// NewKafkaResultProducer creates a producer aimed at kernelq.jobs.results.
func NewKafkaResultProducer(bootstrapServers string) (*KafkaResultProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	return &KafkaResultProducer{
		Producer: producer,
		Topic:    ResultTopic,
	}, nil
}

// PublishResult validates, JSON-encodes, and publishes one worker result event.
//
// Starts kafka.publish under the caller context (typically worker.execute),
// injects W3C trace context into headers, then waits for broker ack.
func (p KafkaResultProducer) PublishResult(ctx context.Context, event WorkerResultEvent) error {
	if ctx == nil {
		ctx = context.Background()
	}
	topic := p.Topic
	ctx, span := telemetry.StartKafkaPublishSpan(ctx, topic, "publish")
	defer span.End()

	log := p.publishLogger(ctx, event)
	log.Info("result publish started", "operation", "result_publish", "status", "started")

	if err := event.Validate(); err != nil {
		telemetry.RecordSpanError(span, err)
		log.Error(
			"result publish failed",
			"operation", "result_publish",
			"status", "failed",
			"error_type", logging.ErrorType(err),
		)
		return err
	}

	jsonPayload, err := event.ToJSON()
	if err != nil {
		telemetry.RecordSpanError(span, err)
		log.Error(
			"result publish failed",
			"operation", "result_publish",
			"status", "failed",
			"error_type", logging.ErrorType(err),
		)
		return err
	}

	headers := []kafka.Header{}
	telemetry.InjectKafkaContext(ctx, &headers)

	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key:     []byte(event.JobID),
		Value:   []byte(jsonPayload),
		Headers: headers,
	}

	deliveryChan := make(chan kafka.Event, 1)
	if err := p.Producer.Produce(message, deliveryChan); err != nil {
		err = fmt.Errorf("produce result event: %w", err)
		telemetry.RecordSpanError(span, err)
		log.Error(
			"result publish failed",
			"operation", "result_publish",
			"status", "failed",
			"error_type", logging.ErrorType(err),
		)
		return err
	}

	select {
	case ev := <-deliveryChan:
		msg, ok := ev.(*kafka.Message)
		if !ok {
			err := fmt.Errorf("deliver result event: unexpected event type %T", ev)
			telemetry.RecordSpanError(span, err)
			log.Error(
				"result publish failed",
				"operation", "result_publish",
				"status", "failed",
				"error_type", logging.ErrorType(err),
			)
			return err
		}
		if msg.TopicPartition.Error != nil {
			err := fmt.Errorf("deliver result event: %w", msg.TopicPartition.Error)
			telemetry.RecordSpanError(span, err)
			log.Error(
				"result publish failed",
				"operation", "result_publish",
				"status", "failed",
				"error_type", logging.ErrorType(err),
			)
			return err
		}
		telemetry.SetKafkaDeliveryAttributes(
			span,
			msg.TopicPartition.Partition,
			int64(msg.TopicPartition.Offset),
		)
		log.Info("result publish completed", "operation", "result_publish", "status", "success")
		return nil
	case <-time.After(time.Duration(resultFlushTimeoutMs) * time.Millisecond):
		err := fmt.Errorf("deliver result event: timed out after %dms", resultFlushTimeoutMs)
		telemetry.RecordSpanError(span, err)
		log.Error(
			"result publish failed",
			"operation", "result_publish",
			"status", "failed",
			"error_type", "timeout",
		)
		return err
	}
}

func (p KafkaResultProducer) publishLogger(ctx context.Context, event WorkerResultEvent) *slog.Logger {
	if p.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	log := logging.WithComponent(p.Logger, "worker", "result_publish")
	if event.JobID != "" {
		log = log.With("job_id", event.JobID)
	}
	return logging.WithTraceContext(ctx, log)
}
