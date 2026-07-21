// Package worker holds types and logic for the KernelQ worker plane.
//
// Instrumented dispatch publish for Kafka (Day 122). The control plane also
// publishes to kernelq.jobs.dispatch; this Go producer is used by smoke tests
// and local tooling so W3C headers are injected without a live Python OTel SDK.
package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const dispatchFlushTimeoutMs = 5000

// KafkaDispatchProducer publishes DispatchEvent JSON to a Kafka dispatch topic
// with kafka.publish spans and W3C header injection.
type KafkaDispatchProducer struct {
	Producer KafkaProducerClient
	Topic    string
}

// NewKafkaDispatchProducer creates a producer aimed at kernelq.jobs.dispatch.
func NewKafkaDispatchProducer(bootstrapServers string) (*KafkaDispatchProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &KafkaDispatchProducer{
		Producer: producer,
		Topic:    DispatchTopic,
	}, nil
}

// Close flushes and closes the underlying producer when it supports Close.
func (p *KafkaDispatchProducer) Close() {
	if p == nil || p.Producer == nil {
		return
	}
	if closer, ok := p.Producer.(interface{ Close() }); ok {
		closer.Close()
	}
}

// PublishDispatch publishes one dispatch event with trace-context headers.
func (p *KafkaDispatchProducer) PublishDispatch(ctx context.Context, event DispatchEvent) error {
	if p == nil || p.Producer == nil {
		return fmt.Errorf("dispatch producer is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := event.Validate(); err != nil {
		return err
	}

	topic := p.Topic
	if topic == "" {
		topic = DispatchTopic
	}

	ctx, span := telemetry.StartKafkaPublishSpan(ctx, topic, "publish")
	defer span.End()

	payload, err := event.ToJSON()
	if err != nil {
		telemetry.RecordSpanError(span, err)
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
		Value:   payload,
		Headers: headers,
	}

	deliveryChan := make(chan kafka.Event, 1)
	if err := p.Producer.Produce(message, deliveryChan); err != nil {
		err = fmt.Errorf("produce dispatch event: %w", err)
		telemetry.RecordSpanError(span, err)
		return err
	}

	select {
	case ev := <-deliveryChan:
		msg, ok := ev.(*kafka.Message)
		if !ok {
			err := fmt.Errorf("deliver dispatch event: unexpected event type %T", ev)
			telemetry.RecordSpanError(span, err)
			return err
		}
		if msg.TopicPartition.Error != nil {
			err := fmt.Errorf("deliver dispatch event: %w", msg.TopicPartition.Error)
			telemetry.RecordSpanError(span, err)
			return err
		}
		telemetry.SetKafkaDeliveryAttributes(
			span,
			msg.TopicPartition.Partition,
			int64(msg.TopicPartition.Offset),
		)
		return nil
	case <-time.After(time.Duration(dispatchFlushTimeoutMs) * time.Millisecond):
		err := fmt.Errorf("deliver dispatch event: timed out after %dms", dispatchFlushTimeoutMs)
		telemetry.RecordSpanError(span, err)
		return err
	}
}
