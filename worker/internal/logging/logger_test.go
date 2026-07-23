package logging_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/logging"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("KERNELQ_LOG_LEVEL", "")
	t.Setenv("KERNELQ_LOG_FORMAT", "")
	t.Setenv("KERNELQ_SERVICE_NAME", "")
	t.Setenv("KERNELQ_ENVIRONMENT", "")
	t.Setenv("KERNELQ_VERSION", "")
	cfg, err := logging.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Level != "info" || cfg.Format != "json" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestNewJSONIncludesBaseFields(t *testing.T) {
	var buf bytes.Buffer
	cfg := logging.Config{
		Level:       "info",
		Format:      "json",
		ServiceName: "kernelq-worker",
		Environment: "local",
		Version:     "dev",
	}
	logger, err := logging.New(cfg, &buf)
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("job execution completed", "component", "worker", "operation", "execute", "status", "success")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("json: %v raw=%q", err, buf.String())
	}
	for _, key := range []string{"timestamp", "level", "message", "service", "environment", "version"} {
		if _, ok := entry[key]; !ok {
			t.Fatalf("missing %s in %v", key, entry)
		}
	}
	if entry["service"] != "kernelq-worker" || entry["message"] != "job execution completed" {
		t.Fatalf("unexpected entry: %v", entry)
	}
	if strings.Contains(buf.String(), "authorization") || strings.Contains(buf.String(), "raw_payload") {
		t.Fatal("forbidden fields present")
	}
}

func TestValidateRejectsBadLevel(t *testing.T) {
	err := logging.Config{Level: "verbose", Format: "json", ServiceName: "s", Environment: "e", Version: "v"}.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWithTraceContextAttachesIDs(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := otel.Tracer("test").Start(context.Background(), "root")
	defer span.End()
	wantTrace := strings.ToLower(span.SpanContext().TraceID().String())
	wantSpan := strings.ToLower(span.SpanContext().SpanID().String())

	var buf bytes.Buffer
	logger, err := logging.New(logging.Config{
		Level: "info", Format: "json", ServiceName: "kernelq-worker", Environment: "local", Version: "dev",
	}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	logging.WithTraceContext(ctx, logger).Info("correlated")

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["trace_id"] != wantTrace || entry["span_id"] != wantSpan {
		t.Fatalf("trace fields = %v %v, want %s %s", entry["trace_id"], entry["span_id"], wantTrace, wantSpan)
	}
}

func TestWithJobAttachesFields(t *testing.T) {
	var buf bytes.Buffer
	logger, err := logging.New(logging.Config{
		Level: "info", Format: "json", ServiceName: "kernelq-worker", Environment: "local", Version: "dev",
	}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	logging.WithJob(logger, "job-123", 1).Info("started")
	var entry map[string]any
	_ = json.Unmarshal(buf.Bytes(), &entry)
	if entry["job_id"] != "job-123" {
		t.Fatalf("job_id=%v", entry["job_id"])
	}
	if int(entry["attempt"].(float64)) != 1 {
		t.Fatalf("attempt=%v", entry["attempt"])
	}
}
