package faults

import (
	"context"
	"log/slog"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/metrics"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// LoggingObserver emits structured WARN logs and resilience metrics (no payloads).
type LoggingObserver struct {
	Logger *slog.Logger
}

// OnInject implements Observer.
func (o LoggingObserver) OnInject(ctx context.Context, point Point, mode Mode, remaining int) {
	logger := o.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn(
		"test fault injected",
		"component", "fault_injector",
		"operation", string(point),
		"fault_mode", string(mode),
		"remaining", remaining,
		"status", "injected",
	)

	_ = metrics.IncFaultInjection(string(point), string(mode))

	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.AddEvent("kernelq.fault.injected", trace.WithAttributes(
			attribute.String("fault_point", string(point)),
			attribute.String("fault_mode", string(mode)),
			attribute.Int("remaining", remaining),
		))
	}
}
