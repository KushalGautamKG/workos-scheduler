// Package logging provides KernelQ's structured slog configuration (Day 127).
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

const (
	DefaultLevel       = "info"
	DefaultFormat      = FormatJSON
	DefaultServiceName = "kernelq-worker"
	DefaultEnvironment = "local"
	DefaultVersion     = "dev"

	FormatJSON = "json"
	FormatText = "text"
)

// Config holds process logging settings.
type Config struct {
	Level       string
	Format      string
	ServiceName string
	Environment string
	Version     string
}

// LoadConfig reads KERNELQ_LOG_* and related environment variables.
func LoadConfig() (Config, error) {
	cfg := Config{
		Level:       DefaultLevel,
		Format:      DefaultFormat,
		ServiceName: DefaultServiceName,
		Environment: DefaultEnvironment,
		Version:     DefaultVersion,
	}
	if v := strings.TrimSpace(os.Getenv("KERNELQ_LOG_LEVEL")); v != "" {
		cfg.Level = v
	}
	if v := strings.TrimSpace(os.Getenv("KERNELQ_LOG_FORMAT")); v != "" {
		cfg.Format = v
	}
	if v := strings.TrimSpace(os.Getenv("KERNELQ_SERVICE_NAME")); v != "" {
		cfg.ServiceName = v
	}
	if v := strings.TrimSpace(os.Getenv("KERNELQ_ENVIRONMENT")); v != "" {
		cfg.Environment = v
	}
	if v := strings.TrimSpace(os.Getenv("KERNELQ_VERSION")); v != "" {
		cfg.Version = v
	}
	return cfg, cfg.Validate()
}

// Validate checks level, format, and required identity fields.
func (cfg Config) Validate() error {
	level := strings.ToLower(strings.TrimSpace(cfg.Level))
	switch level {
	case "debug", "info", "warn", "warning", "error":
	default:
		return fmt.Errorf("unsupported KERNELQ_LOG_LEVEL %q", cfg.Level)
	}
	format := strings.ToLower(strings.TrimSpace(cfg.Format))
	switch format {
	case FormatJSON, FormatText:
	default:
		return fmt.Errorf("unsupported KERNELQ_LOG_FORMAT %q", cfg.Format)
	}
	if strings.TrimSpace(cfg.ServiceName) == "" {
		return fmt.Errorf("service name must be non-empty")
	}
	if strings.TrimSpace(cfg.Environment) == "" {
		return fmt.Errorf("environment must be non-empty")
	}
	if strings.TrimSpace(cfg.Version) == "" {
		return fmt.Errorf("version must be non-empty")
	}
	return nil
}

// New builds a slog.Logger writing JSON or text to output.
func New(cfg Config, output io.Writer) (*slog.Logger, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if output == nil {
		output = os.Stdout
	}

	level := parseLevel(cfg.Level)
	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Key = "timestamp"
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.UTC().Format(time.RFC3339Nano))
				}
			}
			if a.Key == slog.MessageKey {
				a.Key = "message"
			}
			if a.Key == slog.LevelKey {
				a.Key = "level"
				a.Value = slog.StringValue(strings.ToUpper(a.Value.String()))
			}
			return a
		},
	}

	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(cfg.Format)) {
	case FormatText:
		handler = slog.NewTextHandler(output, opts)
	default:
		handler = slog.NewJSONHandler(output, opts)
	}

	logger := slog.New(handler).With(
		"service", cfg.ServiceName,
		"environment", cfg.Environment,
		"version", cfg.Version,
	)
	return logger, nil
}

func parseLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
