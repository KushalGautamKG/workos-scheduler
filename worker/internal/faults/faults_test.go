package faults_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/faults"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/logging"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/metrics"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestLoadConfigDisabledByDefault(t *testing.T) {
	t.Setenv("KERNELQ_FAULTS_ENABLED", "")
	t.Setenv("KERNELQ_FAULT_POINT", "")
	cfg, err := faults.LoadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Enabled {
		t.Fatal("expected faults disabled by default")
	}
	inj, err := faults.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := inj.Inject(context.Background(), faults.PointBeforeExecute); err != nil {
		t.Fatalf("noop should not error: %v", err)
	}
}

func TestLoadConfigRejectsProduction(t *testing.T) {
	t.Setenv("KERNELQ_FAULTS_ENABLED", "true")
	t.Setenv("KERNELQ_ENVIRONMENT", "production")
	t.Setenv("KERNELQ_FAULT_POINT", "before_execute")
	_, err := faults.LoadConfig()
	if err == nil {
		t.Fatal("expected production rejection")
	}
}

func TestLoadConfigRequiresExplicitNonProduction(t *testing.T) {
	t.Setenv("KERNELQ_FAULTS_ENABLED", "true")
	t.Setenv("KERNELQ_ENVIRONMENT", "staging")
	t.Setenv("KERNELQ_FAULT_POINT", "before_execute")
	_, err := faults.LoadConfig()
	if err == nil {
		t.Fatal("expected staging rejection")
	}
}

func TestConfigurableInjectorBoundedError(t *testing.T) {
	metrics.ResetForTest()
	inj, err := faults.New(faults.Config{
		Enabled: true,
		Point:   faults.PointBeforeExecute,
		Mode:    faults.ModeError,
		Count:   1,
	}, faults.WithObserver(faults.LoggingObserver{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := inj.Inject(context.Background(), faults.PointBeforeClaim); err != nil {
		t.Fatalf("wrong point should noop: %v", err)
	}
	err = inj.Inject(context.Background(), faults.PointBeforeExecute)
	if !errors.Is(err, faults.ErrInjected) {
		t.Fatalf("want ErrInjected, got %v", err)
	}
	if err := inj.Inject(context.Background(), faults.PointBeforeExecute); err != nil {
		t.Fatalf("count exhausted should noop: %v", err)
	}
	if metrics.FaultInjections("before_execute", "error") != 1 {
		t.Fatalf("metric = %d", metrics.FaultInjections("before_execute", "error"))
	}
}

func TestConfigurableInjectorDelayCancelable(t *testing.T) {
	inj, err := faults.New(faults.Config{
		Enabled: true,
		Point:   faults.PointAfterClaim,
		Mode:    faults.ModeDelay,
		Count:   1,
		Delay:   5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = inj.Inject(ctx, faults.PointAfterClaim)
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("want cancel/deadline, got %v", err)
	}
}

func TestObserverEmitsLogAndTraceEvent(t *testing.T) {
	metrics.ResetForTest()
	var buf bytes.Buffer
	logger, err := logging.New(logging.Config{
		Level: "warn", Format: "json", ServiceName: "kernelq-worker", Environment: "test", Version: "dev",
	}, &buf)
	if err != nil {
		t.Fatal(err)
	}
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := otel.Tracer("faults-test").Start(context.Background(), "test")
	defer span.End()

	inj, err := faults.New(faults.Config{
		Enabled: true,
		Point:   faults.PointBeforeClaim,
		Mode:    faults.ModeError,
		Count:   1,
	}, faults.WithObserver(faults.LoggingObserver{Logger: logger}))
	if err != nil {
		t.Fatal(err)
	}
	_ = inj.Inject(ctx, faults.PointBeforeClaim)
	if !strings.Contains(buf.String(), "test fault injected") {
		t.Fatalf("missing log: %s", buf.String())
	}
	if strings.Contains(buf.String(), "payload") {
		t.Fatal("must not log payloads")
	}
	span.End()
	spans := exporter.GetSpans()
	if len(spans) == 0 {
		t.Fatal("expected spans")
	}
	found := false
	for _, ev := range spans[0].Events {
		if ev.Name == "kernelq.fault.injected" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected fault span event")
	}
}
