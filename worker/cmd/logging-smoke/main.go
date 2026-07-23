// Command logging-smoke emits sample structured JSON logs for Day 127 smoke.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/logging"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func main() {
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
		fail(err)
	}

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	ctx, span := otel.Tracer("logging-smoke").Start(context.Background(), "smoke")
	defer span.End()
	wantTrace := strings.ToLower(span.SpanContext().TraceID().String())

	store := worker.NewInMemoryIdempotencyStore()
	handler := &worker.DispatchEventHandler{
		Executor:         successExecutor{},
		IdempotencyStore: store,
		IdempotencyTTL:   0,
		WorkerName:       "logging-smoke",
		Logger:           logger,
	}

	event := worker.DispatchEvent{
		EventType: "job.dispatch",
		JobID:     "job-smoke-127",
		TenantID:  "tenant-a",
		Priority:  1,
		Attempt:   1,
		State:     "dispatched",
		Payload:   map[string]string{"ok": "true"},
	}
	if _, err := handler.Handle(ctx, event); err != nil {
		fail(err)
	}
	// Duplicate path (WARN, not crash).
	if _, err := handler.Handle(ctx, event); err != nil {
		fail(err)
	}

	out := buf.String()
	if _, err := os.Stdout.WriteString(out); err != nil {
		fail(err)
	}

	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) == 0 {
		fail(fmt.Errorf("no log lines"))
	}
	foundTrace := false
	foundJob := false
	for _, line := range lines {
		var entry map[string]any
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			fail(fmt.Errorf("invalid json: %w", err))
		}
		for _, key := range []string{"timestamp", "level", "message", "service", "environment", "version"} {
			if _, ok := entry[key]; !ok {
				fail(fmt.Errorf("missing %s in %v", key, entry))
			}
		}
		// Logs attach the active worker.execute child span; trace_id must match the root.
		tid, hasTrace := entry["trace_id"].(string)
		sid, hasSpan := entry["span_id"].(string)
		if hasTrace && hasSpan && tid == wantTrace && sid != "" {
			foundTrace = true
		}
		if entry["job_id"] == "job-smoke-127" {
			if attempt, ok := entry["attempt"].(float64); ok && int(attempt) == 1 {
				foundJob = true
			}
		}
		for _, forbidden := range []string{"authorization", "password", "token", "raw_payload"} {
			if _, ok := entry[forbidden]; ok {
				fail(fmt.Errorf("forbidden field %s present", forbidden))
			}
		}
	}
	if !foundTrace {
		fail(fmt.Errorf("trace_id/span_id not correlated to active trace"))
	}
	if !foundJob {
		fail(fmt.Errorf("job_id/attempt missing"))
	}
	fmt.Fprintln(os.Stderr, "event=logging_smoke_helper success=true")
}

type successExecutor struct{}

func (successExecutor) Execute(task worker.Task) (worker.ExecutionResult, error) {
	return worker.SuccessResult(), nil
}

func fail(err error) {
	fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
	os.Exit(1)
}
