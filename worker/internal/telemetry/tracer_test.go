package telemetry

import (
	"context"
	"testing"
)

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("KERNELQ_OTEL_ENABLED", "")
	t.Setenv("KERNELQ_OTEL_SERVICE_NAME", "")
	t.Setenv("KERNELQ_OTEL_EXPORTER", "")
	t.Setenv("KERNELQ_OTEL_SERVICE_VERSION", "")
	t.Setenv("KERNELQ_OTEL_ENVIRONMENT", "")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !cfg.Enabled {
		t.Fatal("expected enabled=true by default")
	}
	if cfg.ServiceName != DefaultServiceName {
		t.Fatalf("service = %q", cfg.ServiceName)
	}
	if cfg.Exporter != DefaultExporter {
		t.Fatalf("exporter = %q", cfg.Exporter)
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	t.Setenv("KERNELQ_OTEL_ENABLED", "false")
	t.Setenv("KERNELQ_OTEL_SERVICE_NAME", "kernelq-test")
	t.Setenv("KERNELQ_OTEL_EXPORTER", "none")
	t.Setenv("KERNELQ_OTEL_SERVICE_VERSION", "1.2.3")
	t.Setenv("KERNELQ_OTEL_ENVIRONMENT", "test")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Enabled {
		t.Fatal("expected enabled=false")
	}
	if cfg.ServiceName != "kernelq-test" {
		t.Fatalf("service = %q", cfg.ServiceName)
	}
	if cfg.Exporter != ExporterNone {
		t.Fatalf("exporter = %q", cfg.Exporter)
	}
	if cfg.Version != "1.2.3" || cfg.Environment != "test" {
		t.Fatalf("version/env = %q/%q", cfg.Version, cfg.Environment)
	}
}

func TestInvalidExporterRejected(t *testing.T) {
	cfg := Config{
		Enabled:     true,
		ServiceName: "kernelq-worker",
		Exporter:    "otlp",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected invalid exporter error")
	}
}

func TestEnabledStdoutProviderBuildsAndShutdown(t *testing.T) {
	cfg := Config{
		Enabled:     true,
		ServiceName: "kernelq-worker-test",
		Exporter:    ExporterStdout,
		Version:     "test",
		Environment: "test",
	}
	provider, err := NewTracerProvider(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewTracerProvider: %v", err)
	}
	if !provider.Enabled() {
		t.Fatal("expected enabled provider")
	}
	if provider.TracerProvider() == nil {
		t.Fatal("expected tracer provider")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestDisabledProviderBuildsAndShutdown(t *testing.T) {
	cfg := Config{
		Enabled:     false,
		ServiceName: "kernelq-worker-test",
		Exporter:    ExporterStdout,
		Version:     "test",
		Environment: "test",
	}
	provider, err := NewTracerProvider(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewTracerProvider: %v", err)
	}
	if provider.Enabled() {
		t.Fatal("expected disabled provider")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestNoneExporterBuilds(t *testing.T) {
	cfg := Config{
		Enabled:     true,
		ServiceName: "kernelq-worker-test",
		Exporter:    ExporterNone,
		Version:     "test",
		Environment: "test",
	}
	provider, err := NewTracerProvider(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewTracerProvider: %v", err)
	}
	if provider.Enabled() {
		t.Fatal("none exporter should not be Enabled export path")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestNewTracerProviderRejectsInvalidConfig(t *testing.T) {
	_, err := NewTracerProvider(context.Background(), Config{
		Enabled:     true,
		ServiceName: "x",
		Exporter:    "jaeger",
	})
	if err == nil {
		t.Fatal("expected invalid config error")
	}
}
