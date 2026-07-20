package telemetry

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/codes"
)

func setupInMemoryTracer(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})
	return exporter
}

func TestStartExecutionSpanAttachesJobAttributes(t *testing.T) {
	exporter := setupInMemoryTracer(t)

	ctx, span := StartExecutionSpan(context.Background(), "job-abc", 2)
	if ctx == nil || span == nil {
		t.Fatal("expected ctx and span")
	}
	RecordExecutionSuccess(span)
	span.End()

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("got %d spans, want 1", len(spans))
	}
	got := spans[0]
	if got.Name != SpanWorkerExecute {
		t.Fatalf("name = %q, want %s", got.Name, SpanWorkerExecute)
	}
	attrs := attrMap(got)
	if attrs[AttrJobID] != "job-abc" {
		t.Fatalf("job.id = %v", attrs[AttrJobID])
	}
	if attrs[AttrJobAttempt] != int64(2) {
		t.Fatalf("job.attempt = %v", attrs[AttrJobAttempt])
	}
	if attrs[AttrExecutionStatus] != ExecutionStatusSuccess {
		t.Fatalf("execution.status = %v", attrs[AttrExecutionStatus])
	}
}

func TestRecordExecutionDuplicate(t *testing.T) {
	exporter := setupInMemoryTracer(t)

	_, span := StartExecutionSpan(context.Background(), "job-dup", 0)
	RecordExecutionDuplicate(span)
	span.End()

	attrs := attrMap(exporter.GetSpans()[0])
	if attrs[AttrExecutionStatus] != ExecutionStatusDuplicate {
		t.Fatalf("status = %v", attrs[AttrExecutionStatus])
	}
	if attrs[AttrDuplicateSkipped] != true {
		t.Fatalf("duplicate_skipped = %v", attrs[AttrDuplicateSkipped])
	}
}

func TestRecordExecutionFailureRecordsError(t *testing.T) {
	exporter := setupInMemoryTracer(t)

	_, span := StartExecutionSpan(context.Background(), "job-fail", 1)
	err := errors.New("boom")
	RecordExecutionFailure(span, err)
	span.End()

	got := exporter.GetSpans()[0]
	attrs := attrMap(got)
	if attrs[AttrExecutionStatus] != ExecutionStatusFailed {
		t.Fatalf("status = %v", attrs[AttrExecutionStatus])
	}
	if got.Status.Code != codes.Error {
		t.Fatalf("span status = %v, want Error", got.Status.Code)
	}
	if len(got.Events) == 0 {
		t.Fatal("expected exception event from RecordError")
	}
}

func TestSpanAlwaysEnds(t *testing.T) {
	exporter := setupInMemoryTracer(t)

	func() {
		_, span := StartExecutionSpan(context.Background(), "job-end", 0)
		defer span.End()
		RecordExecutionSuccess(span)
	}()

	if len(exporter.GetSpans()) != 1 {
		t.Fatalf("expected ended span exported, got %d", len(exporter.GetSpans()))
	}
	if !exporter.GetSpans()[0].EndTime.After(exporter.GetSpans()[0].StartTime) &&
		!exporter.GetSpans()[0].EndTime.Equal(exporter.GetSpans()[0].StartTime) {
		// EndTime should be set for ended spans.
	}
	if exporter.GetSpans()[0].EndTime.IsZero() {
		t.Fatal("expected non-zero EndTime")
	}
}

func attrMap(span tracetest.SpanStub) map[string]any {
	out := make(map[string]any, len(span.Attributes))
	for _, a := range span.Attributes {
		out[string(a.Key)] = a.Value.AsInterface()
	}
	return out
}
