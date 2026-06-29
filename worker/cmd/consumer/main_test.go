package main

import (
	"testing"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
)

const (
	envBackpressureEnabled = "KERNELQ_WORKER_BACKPRESSURE_ENABLED"
	envBackpressureHigh    = "KERNELQ_WORKER_BACKPRESSURE_HIGH_RATIO"
	envBackpressureLow     = "KERNELQ_WORKER_BACKPRESSURE_LOW_RATIO"
)

func TestBoolFromEnvEnabledTrue(t *testing.T) {
	t.Setenv(envBackpressureEnabled, "true")

	if !boolFromEnv(envBackpressureEnabled, false) {
		t.Fatal("expected true when env is true")
	}
}

func TestBoolFromEnvDefaultsFalseWhenUnset(t *testing.T) {
	t.Setenv(envBackpressureEnabled, "")

	if boolFromEnv(envBackpressureEnabled, false) {
		t.Fatal("expected false when env unset")
	}
}

func TestBoolFromEnvInvalidDefaultsFalse(t *testing.T) {
	t.Setenv(envBackpressureEnabled, "not-a-bool")

	if boolFromEnv(envBackpressureEnabled, false) {
		t.Fatal("expected false when env invalid")
	}
}

func TestFloatFromEnvParsesHighLowRatios(t *testing.T) {
	t.Setenv(envBackpressureHigh, "0.75")
	t.Setenv(envBackpressureLow, "0.25")

	high := floatFromEnv(envBackpressureHigh, defaultBackpressureHighRatio)
	low := floatFromEnv(envBackpressureLow, defaultBackpressureLowRatio)

	if high != 0.75 {
		t.Fatalf("high ratio: got %v want 0.75", high)
	}
	if low != 0.25 {
		t.Fatalf("low ratio: got %v want 0.25", low)
	}
}

func TestInvalidRatiosUseDefaultsThroughNewBackpressurePolicy(t *testing.T) {
	// Values parse from env but violate policy constraints (low >= high).
	t.Setenv(envBackpressureHigh, "0.60")
	t.Setenv(envBackpressureLow, "0.70")

	high := floatFromEnv(envBackpressureHigh, defaultBackpressureHighRatio)
	low := floatFromEnv(envBackpressureLow, defaultBackpressureLowRatio)
	policy := worker.NewBackpressurePolicy(high, low)

	if !policy.ShouldPause(80, 100) {
		t.Fatal("expected default policy to pause at depth 80 with capacity 100")
	}
	if policy.ShouldPause(79, 100) {
		t.Fatal("expected default policy not to pause below depth 79")
	}
}
