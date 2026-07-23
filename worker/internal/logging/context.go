package logging

import (
	"context"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

// WithTraceContext returns a logger enriched with trace_id and span_id when valid.
func WithTraceContext(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return logger
	}
	if ctx == nil {
		return logger
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return logger
	}
	return logger.With(
		"trace_id", strings.ToLower(sc.TraceID().String()),
		"span_id", strings.ToLower(sc.SpanID().String()),
	)
}

// WithJob attaches job_id and attempt (no payloads).
func WithJob(logger *slog.Logger, jobID string, attempt int) *slog.Logger {
	if logger == nil {
		return logger
	}
	attrs := make([]any, 0, 4)
	if strings.TrimSpace(jobID) != "" {
		attrs = append(attrs, "job_id", jobID)
	}
	attrs = append(attrs, "attempt", attempt)
	return logger.With(attrs...)
}

// WithComponent attaches component and optional operation.
func WithComponent(logger *slog.Logger, component, operation string) *slog.Logger {
	if logger == nil {
		return logger
	}
	attrs := make([]any, 0, 4)
	if strings.TrimSpace(component) != "" {
		attrs = append(attrs, "component", component)
	}
	if strings.TrimSpace(operation) != "" {
		attrs = append(attrs, "operation", operation)
	}
	if len(attrs) == 0 {
		return logger
	}
	return logger.With(attrs...)
}

// ErrorType returns a short safe error class name (not the full message).
func ErrorType(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	s = strings.ToLower(s)
	switch {
	case strings.Contains(s, "timeout"):
		return "timeout"
	case strings.Contains(s, "unavailable"):
		return "unavailable"
	case strings.Contains(s, "duplicate"):
		return "duplicate"
	case strings.Contains(s, "invalid"):
		return "invalid"
	default:
		return "error"
	}
}
