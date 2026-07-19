// Package config holds process configuration parsers (environment → structs).
package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	DefaultGRPCAddr            = "127.0.0.1:50051"
	DefaultGRPCShutdownTimeout = 10 * time.Second
	DefaultGRPCRequestTimeout  = 5 * time.Second
)

// GRPCConfig is runtime configuration for cmd/grpc-server and gRPC clients.
type GRPCConfig struct {
	Addr            string
	ShutdownTimeout time.Duration
	RequestTimeout  time.Duration
}

// LoadGRPCConfig reads KERNELQ_GRPC_* environment variables with defaults.
func LoadGRPCConfig() (GRPCConfig, error) {
	cfg := GRPCConfig{
		Addr:            strings.TrimSpace(os.Getenv("KERNELQ_GRPC_ADDR")),
		ShutdownTimeout: DefaultGRPCShutdownTimeout,
		RequestTimeout:  DefaultGRPCRequestTimeout,
	}
	if cfg.Addr == "" {
		cfg.Addr = DefaultGRPCAddr
	}

	if raw := strings.TrimSpace(os.Getenv("KERNELQ_GRPC_SHUTDOWN_TIMEOUT")); raw != "" {
		duration, err := time.ParseDuration(raw)
		if err != nil {
			return GRPCConfig{}, fmt.Errorf("KERNELQ_GRPC_SHUTDOWN_TIMEOUT: %w", err)
		}
		cfg.ShutdownTimeout = duration
	}

	if raw := strings.TrimSpace(os.Getenv("KERNELQ_GRPC_REQUEST_TIMEOUT")); raw != "" {
		duration, err := time.ParseDuration(raw)
		if err != nil {
			return GRPCConfig{}, fmt.Errorf("KERNELQ_GRPC_REQUEST_TIMEOUT: %w", err)
		}
		cfg.RequestTimeout = duration
	}

	if err := cfg.Validate(); err != nil {
		return GRPCConfig{}, err
	}
	return cfg, nil
}

// Validate checks that address and durations are usable.
func (cfg GRPCConfig) Validate() error {
	if strings.TrimSpace(cfg.Addr) == "" {
		return fmt.Errorf("grpc addr must not be empty")
	}
	if cfg.ShutdownTimeout <= 0 {
		return fmt.Errorf("grpc shutdown timeout must be positive, got %s", cfg.ShutdownTimeout)
	}
	if cfg.RequestTimeout <= 0 {
		return fmt.Errorf("grpc request timeout must be positive, got %s", cfg.RequestTimeout)
	}
	return nil
}
