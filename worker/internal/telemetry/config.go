// Package telemetry provides KernelQ's shared OpenTelemetry foundation (Day 119).
//
// One tracer provider is configured at process start. Spans and OTLP come later;
// today we only wire lifecycle (stdout or none).
package telemetry

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	DefaultServiceName = "kernelq-worker"
	DefaultExporter    = ExporterStdout
	DefaultEnvironment = "local"
	DefaultVersion     = "dev"

	ExporterStdout = "stdout"
	ExporterNone   = "none"
)

// Config holds OpenTelemetry process settings.
type Config struct {
	Enabled     bool
	ServiceName string
	Exporter    string
	Version     string
	Environment string
}

// LoadConfig reads KERNELQ_OTEL_* environment variables with defaults.
func LoadConfig() (Config, error) {
	cfg := Config{
		Enabled:     true,
		ServiceName: DefaultServiceName,
		Exporter:    DefaultExporter,
		Version:     DefaultVersion,
		Environment: DefaultEnvironment,
	}

	if raw := strings.TrimSpace(os.Getenv("KERNELQ_OTEL_ENABLED")); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			return Config{}, fmt.Errorf("KERNELQ_OTEL_ENABLED: %w", err)
		}
		cfg.Enabled = enabled
	}

	if name := strings.TrimSpace(os.Getenv("KERNELQ_OTEL_SERVICE_NAME")); name != "" {
		cfg.ServiceName = name
	}

	if exporter := strings.ToLower(strings.TrimSpace(os.Getenv("KERNELQ_OTEL_EXPORTER"))); exporter != "" {
		cfg.Exporter = exporter
	}

	if version := strings.TrimSpace(os.Getenv("KERNELQ_OTEL_SERVICE_VERSION")); version != "" {
		cfg.Version = version
	}

	if env := strings.TrimSpace(os.Getenv("KERNELQ_OTEL_ENVIRONMENT")); env != "" {
		cfg.Environment = env
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks exporter and required identity fields.
func (cfg Config) Validate() error {
	if strings.TrimSpace(cfg.ServiceName) == "" {
		return fmt.Errorf("otel service name must not be empty")
	}
	switch strings.ToLower(strings.TrimSpace(cfg.Exporter)) {
	case ExporterStdout, ExporterNone:
		return nil
	default:
		return fmt.Errorf("otel exporter must be %q or %q, got %q", ExporterStdout, ExporterNone, cfg.Exporter)
	}
}
