package telemetry

import (
	"context"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	// SpanKafkaPublish is the producer-side messaging span.
	SpanKafkaPublish = "kafka.publish"
	// SpanKafkaProcess is the consumer-side messaging span.
	SpanKafkaProcess = "kafka.process"
)

// KafkaHeaderCarrier adapts confluent-kafka-go headers to TextMapCarrier.
type KafkaHeaderCarrier struct {
	Headers *[]kafka.Header
}

// Get returns the first header value matching key (case-insensitive).
func (c KafkaHeaderCarrier) Get(key string) string {
	if c.Headers == nil || *c.Headers == nil {
		return ""
	}
	for _, h := range *c.Headers {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

// Set replaces an existing matching header or appends a new one.
func (c KafkaHeaderCarrier) Set(key, value string) {
	if c.Headers == nil {
		return
	}
	if *c.Headers == nil {
		*c.Headers = make([]kafka.Header, 0, 1)
	}
	for i := range *c.Headers {
		if strings.EqualFold((*c.Headers)[i].Key, key) {
			(*c.Headers)[i] = kafka.Header{Key: key, Value: []byte(value)}
			return
		}
	}
	*c.Headers = append(*c.Headers, kafka.Header{Key: key, Value: []byte(value)})
}

// Keys returns header names currently present.
func (c KafkaHeaderCarrier) Keys() []string {
	if c.Headers == nil || *c.Headers == nil {
		return nil
	}
	keys := make([]string, 0, len(*c.Headers))
	for _, h := range *c.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// InjectKafkaContext writes W3C trace context into Kafka headers via the
// globally configured TextMapPropagator. Never panics on nil headers.
func InjectKafkaContext(ctx context.Context, headers *[]kafka.Header) {
	if ctx == nil || headers == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, KafkaHeaderCarrier{Headers: headers})
}

// ExtractKafkaContext reads W3C trace context from Kafka headers.
// Missing or malformed propagation headers yield a usable context (no panic).
func ExtractKafkaContext(ctx context.Context, headers []kafka.Header) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(headers) == 0 {
		return ctx
	}
	carrier := headers
	return otel.GetTextMapPropagator().Extract(ctx, KafkaHeaderCarrier{Headers: &carrier})
}

// StartKafkaPublishSpan starts a producer span (SpanKindProducer).
func StartKafkaPublishSpan(
	ctx context.Context,
	topic string,
	operation string,
) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(operation) == "" {
		operation = "publish"
	}
	tracer := otel.Tracer(TracerName)
	return tracer.Start(
		ctx,
		SpanKafkaPublish,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation.name", operation),
		),
	)
}

// StartKafkaProcessSpan starts a consumer span (SpanKindConsumer).
func StartKafkaProcessSpan(
	ctx context.Context,
	topic string,
	partition int32,
	offset int64,
) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	tracer := otel.Tracer(TracerName)
	return tracer.Start(
		ctx,
		SpanKafkaProcess,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", topic),
			attribute.String("messaging.operation.name", "process"),
			attribute.Int("messaging.kafka.partition", int(partition)),
			attribute.Int64("messaging.kafka.message.offset", offset),
		),
	)
}

// RecordSpanError marks a span as failed.
func RecordSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

// SetKafkaDeliveryAttributes attaches partition/offset after broker ack.
func SetKafkaDeliveryAttributes(span trace.Span, partition int32, offset int64) {
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.Int("messaging.kafka.partition", int(partition)),
		attribute.Int64("messaging.kafka.message.offset", offset),
	)
}
