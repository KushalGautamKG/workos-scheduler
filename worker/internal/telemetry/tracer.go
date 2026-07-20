package telemetry

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Provider wraps the SDK tracer provider (or a no-op) and exposes Shutdown.
type Provider struct {
	sdk     *sdktrace.TracerProvider
	noop    trace.TracerProvider
	enabled bool
}

// NewTracerProvider builds a shared tracer provider from config.
//
// - enabled=false or exporter=none → no-op global provider (no export)
// - exporter=stdout → SDK provider with batch processor + stdout exporter
//
// Registers the provider globally via otel.SetTracerProvider and installs the
// W3C Trace Context (+ Baggage) TextMapPropagator so otelgrpc can inject and
// extract trace context across the gRPC boundary.
func NewTracerProvider(ctx context.Context, cfg Config) (*Provider, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	installPropagator()

	exporterName := strings.ToLower(strings.TrimSpace(cfg.Exporter))
	if !cfg.Enabled || exporterName == ExporterNone {
		np := noop.NewTracerProvider()
		otel.SetTracerProvider(np)
		return &Provider{noop: np, enabled: false}, nil
	}

	res, err := NewResource(ctx, cfg)
	if err != nil {
		return nil, err
	}

	exporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, fmt.Errorf("otel stdout exporter: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)

	return &Provider{sdk: tp, enabled: true}, nil
}

func installPropagator() {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

// Enabled reports whether spans are exported (stdout path).
func (p *Provider) Enabled() bool {
	return p != nil && p.enabled
}

// TracerProvider returns the underlying trace.TracerProvider.
func (p *Provider) TracerProvider() trace.TracerProvider {
	if p == nil {
		return noop.NewTracerProvider()
	}
	if p.sdk != nil {
		return p.sdk
	}
	if p.noop != nil {
		return p.noop
	}
	return noop.NewTracerProvider()
}

// Shutdown flushes exporters when using the SDK provider.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.sdk == nil {
		return nil
	}
	return p.sdk.Shutdown(ctx)
}
