package telemetry_test

import (
	"context"
	"strings"
	"testing"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func setupKafkaPropagator(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	// Day 122: TraceContext is required. Baggage matches Day 121 composite;
	// tests assert traceparent, not baggage.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return exporter
}

func TestInjectKafkaContextWritesTraceparent(t *testing.T) {
	_ = setupKafkaPropagator(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "root")
	defer span.End()

	headers := []kafka.Header{{Key: "x-custom", Value: []byte("keep-me")}}
	telemetry.InjectKafkaContext(ctx, &headers)

	found := false
	for _, h := range headers {
		if strings.EqualFold(h.Key, "traceparent") && len(h.Value) > 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected traceparent header, got %#v", headers)
	}
	preserved := false
	for _, h := range headers {
		if h.Key == "x-custom" && string(h.Value) == "keep-me" {
			preserved = true
		}
	}
	if !preserved {
		t.Fatal("unrelated header was not preserved")
	}
}

func TestInjectKafkaContextReplacesMatchingHeader(t *testing.T) {
	_ = setupKafkaPropagator(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "root")
	defer span.End()

	headers := []kafka.Header{}
	telemetry.InjectKafkaContext(ctx, &headers)
	firstCount := countHeaderKey(headers, "traceparent")
	telemetry.InjectKafkaContext(ctx, &headers)
	secondCount := countHeaderKey(headers, "traceparent")
	if firstCount != 1 || secondCount != 1 {
		t.Fatalf("traceparent count grew: first=%d second=%d headers=%#v", firstCount, secondCount, headers)
	}
}

func TestExtractKafkaContextMatchesInjectedTrace(t *testing.T) {
	_ = setupKafkaPropagator(t)
	ctx, span := otel.Tracer("test").Start(context.Background(), "root")
	defer span.End()
	want := span.SpanContext().TraceID()

	headers := []kafka.Header{}
	telemetry.InjectKafkaContext(ctx, &headers)

	extracted := telemetry.ExtractKafkaContext(context.Background(), headers)
	sc := trace.SpanContextFromContext(extracted)
	if !sc.IsValid() {
		t.Fatal("extracted span context is invalid")
	}
	if !sc.IsRemote() {
		t.Fatal("extracted span context should be remote")
	}
	if sc.TraceID() != want {
		t.Fatalf("trace id = %s, want %s", sc.TraceID(), want)
	}
}

func TestExtractMissingHeadersIsSafe(t *testing.T) {
	_ = setupKafkaPropagator(t)
	ctx := telemetry.ExtractKafkaContext(context.Background(), nil)
	_, span := telemetry.StartKafkaProcessSpan(ctx, "t", 0, 1)
	span.End()
	if ctx == nil {
		t.Fatal("expected usable context")
	}
}

func TestExtractMalformedTraceparentIsSafe(t *testing.T) {
	exporter := setupKafkaPropagator(t)
	headers := []kafka.Header{
		{Key: "traceparent", Value: []byte("not-a-valid-traceparent")},
		{Key: "x-other", Value: []byte("ok")},
	}
	ctx := telemetry.ExtractKafkaContext(context.Background(), headers)
	_, span := telemetry.StartKafkaProcessSpan(ctx, "t", 1, 2)
	span.End()

	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected process span")
	}
	if spans[0].Parent.IsValid() && spans[0].Parent.IsRemote() {
		// Malformed should not attach a valid remote parent.
		t.Fatalf("unexpected remote parent from malformed header: %#v", spans[0].Parent)
	}
}

func TestInjectExtractNilHeadersNoPanic(t *testing.T) {
	_ = setupKafkaPropagator(t)
	telemetry.InjectKafkaContext(context.Background(), nil)
	_ = telemetry.ExtractKafkaContext(nil, nil)
}

func TestStartKafkaPublishAndProcessKinds(t *testing.T) {
	exporter := setupKafkaPropagator(t)
	ctx, pub := telemetry.StartKafkaPublishSpan(context.Background(), "kernelq.jobs.dispatch", "publish")
	headers := []kafka.Header{}
	telemetry.InjectKafkaContext(ctx, &headers)
	pub.End()

	parent := telemetry.ExtractKafkaContext(context.Background(), headers)
	_, proc := telemetry.StartKafkaProcessSpan(parent, "kernelq.jobs.dispatch", 0, 42)
	proc.End()

	var sawProducer, sawConsumer bool
	var pubID, procID trace.TraceID
	for _, s := range exporter.GetSpans() {
		switch s.SpanKind {
		case trace.SpanKindProducer:
			sawProducer = true
			pubID = s.SpanContext.TraceID()
		case trace.SpanKindConsumer:
			sawConsumer = true
			procID = s.SpanContext.TraceID()
		}
	}
	if !sawProducer || !sawConsumer {
		t.Fatal("expected producer and consumer spans")
	}
	if pubID != procID {
		t.Fatalf("publish/process traces differ: %s vs %s", pubID, procID)
	}
}

func countHeaderKey(headers []kafka.Header, key string) int {
	n := 0
	for _, h := range headers {
		if strings.EqualFold(h.Key, key) {
			n++
		}
	}
	return n
}
