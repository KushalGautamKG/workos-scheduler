package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadGRPCConfigDefaults(t *testing.T) {
	t.Setenv("KERNELQ_GRPC_ADDR", "")
	t.Setenv("KERNELQ_GRPC_SHUTDOWN_TIMEOUT", "")
	t.Setenv("KERNELQ_GRPC_REQUEST_TIMEOUT", "")
	// Clear may not unset if already empty — force delete.
	_ = os.Unsetenv("KERNELQ_GRPC_ADDR")
	_ = os.Unsetenv("KERNELQ_GRPC_SHUTDOWN_TIMEOUT")
	_ = os.Unsetenv("KERNELQ_GRPC_REQUEST_TIMEOUT")

	cfg, err := LoadGRPCConfig()
	if err != nil {
		t.Fatalf("LoadGRPCConfig: %v", err)
	}
	if cfg.Addr != DefaultGRPCAddr {
		t.Fatalf("addr = %q, want %q", cfg.Addr, DefaultGRPCAddr)
	}
	if cfg.ShutdownTimeout != DefaultGRPCShutdownTimeout {
		t.Fatalf("shutdown = %s", cfg.ShutdownTimeout)
	}
	if cfg.RequestTimeout != DefaultGRPCRequestTimeout {
		t.Fatalf("request = %s", cfg.RequestTimeout)
	}
}

func TestLoadGRPCConfigFromEnv(t *testing.T) {
	t.Setenv("KERNELQ_GRPC_ADDR", "127.0.0.1:60051")
	t.Setenv("KERNELQ_GRPC_SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("KERNELQ_GRPC_REQUEST_TIMEOUT", "250ms")

	cfg, err := LoadGRPCConfig()
	if err != nil {
		t.Fatalf("LoadGRPCConfig: %v", err)
	}
	if cfg.Addr != "127.0.0.1:60051" {
		t.Fatalf("addr = %q", cfg.Addr)
	}
	if cfg.ShutdownTimeout != 3*time.Second {
		t.Fatalf("shutdown = %s", cfg.ShutdownTimeout)
	}
	if cfg.RequestTimeout != 250*time.Millisecond {
		t.Fatalf("request = %s", cfg.RequestTimeout)
	}
}

func TestLoadGRPCConfigRejectsNonPositiveDurations(t *testing.T) {
	t.Setenv("KERNELQ_GRPC_ADDR", "127.0.0.1:50051")
	t.Setenv("KERNELQ_GRPC_SHUTDOWN_TIMEOUT", "0s")
	t.Setenv("KERNELQ_GRPC_REQUEST_TIMEOUT", "")

	if _, err := LoadGRPCConfig(); err == nil {
		t.Fatal("expected error for zero shutdown timeout")
	}
}

func TestValidateRejectsEmptyAddr(t *testing.T) {
	cfg := GRPCConfig{
		Addr:            "   ",
		ShutdownTimeout: time.Second,
		RequestTimeout:  time.Second,
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected empty addr error")
	}
}
