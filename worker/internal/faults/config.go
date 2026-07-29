// Package faults provides deterministic, test-only fault injection (Day 129).
//
// Disabled by default. Requires KERNELQ_FAULTS_ENABLED=true and an explicitly
// non-production KERNELQ_ENVIRONMENT. No shell/eval support.
package faults

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Point is a named injection site in the execution pipeline.
type Point string

const (
	PointBeforeClaim         Point = "before_claim"
	PointAfterClaim          Point = "after_claim"
	PointBeforeExecute       Point = "before_execute"
	PointAfterExecute        Point = "after_execute"
	PointBeforeResultPublish Point = "before_result_publish"
	PointAfterResultPublish  Point = "after_result_publish"
)

// Mode selects how a fault manifests.
type Mode string

const (
	ModeError Mode = "error"
	ModeDelay Mode = "delay"
	ModePanic Mode = "panic"
)

// ErrInjected is returned when ModeError fires (typed, safe to classify).
var ErrInjected = errors.New("kernelq: injected test fault")

// Injector applies bounded, deterministic faults at named points.
type Injector interface {
	Inject(ctx context.Context, point Point) error
}

// Config holds env-driven fault settings.
type Config struct {
	Enabled  bool
	Point    Point
	Mode     Mode
	Count    int
	Delay    time.Duration
	EnvName  string
}

// LoadConfig reads KERNELQ_FAULTS_* / KERNELQ_ENVIRONMENT.
// When faults are disabled, returns a zero Config with Enabled=false (no error).
func LoadConfig() (Config, error) {
	cfg := Config{
		Enabled: false,
		Count:   1,
		Mode:    ModeError,
		EnvName: strings.TrimSpace(os.Getenv("KERNELQ_ENVIRONMENT")),
	}
	if cfg.EnvName == "" {
		cfg.EnvName = "local"
	}

	rawEnabled := strings.TrimSpace(os.Getenv("KERNELQ_FAULTS_ENABLED"))
	if rawEnabled == "" || strings.EqualFold(rawEnabled, "false") || rawEnabled == "0" {
		return cfg, nil
	}
	if !strings.EqualFold(rawEnabled, "true") && rawEnabled != "1" {
		return Config{}, fmt.Errorf("KERNELQ_FAULTS_ENABLED must be true or false, got %q", rawEnabled)
	}

	if isProductionEnvironment(cfg.EnvName) {
		return Config{}, fmt.Errorf(
			"fault injection rejected: KERNELQ_ENVIRONMENT=%q is production",
			cfg.EnvName,
		)
	}
	if !isExplicitNonProduction(cfg.EnvName) {
		return Config{}, fmt.Errorf(
			"fault injection rejected: KERNELQ_ENVIRONMENT=%q is not an explicit non-production value (local|test|dev|development)",
			cfg.EnvName,
		)
	}

	cfg.Enabled = true
	cfg.Point = Point(strings.TrimSpace(os.Getenv("KERNELQ_FAULT_POINT")))
	if cfg.Point == "" {
		return Config{}, fmt.Errorf("KERNELQ_FAULT_POINT is required when faults are enabled")
	}
	if !validPoint(cfg.Point) {
		return Config{}, fmt.Errorf("unsupported KERNELQ_FAULT_POINT %q", cfg.Point)
	}

	mode := strings.TrimSpace(os.Getenv("KERNELQ_FAULT_MODE"))
	if mode == "" {
		mode = string(ModeError)
	}
	cfg.Mode = Mode(strings.ToLower(mode))
	if !validMode(cfg.Mode) {
		return Config{}, fmt.Errorf("unsupported KERNELQ_FAULT_MODE %q", cfg.Mode)
	}

	if raw := strings.TrimSpace(os.Getenv("KERNELQ_FAULT_COUNT")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return Config{}, fmt.Errorf("KERNELQ_FAULT_COUNT must be >= 0, got %q", raw)
		}
		cfg.Count = n
	}

	if raw := strings.TrimSpace(os.Getenv("KERNELQ_FAULT_DELAY_MS")); raw != "" {
		ms, err := strconv.Atoi(raw)
		if err != nil || ms < 0 {
			return Config{}, fmt.Errorf("KERNELQ_FAULT_DELAY_MS must be >= 0, got %q", raw)
		}
		cfg.Delay = time.Duration(ms) * time.Millisecond
	}
	if cfg.Mode == ModeDelay && cfg.Delay <= 0 {
		cfg.Delay = 100 * time.Millisecond
	}

	return cfg, nil
}

func isProductionEnvironment(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "production", "prod", "prd":
		return true
	default:
		return false
	}
}

func isExplicitNonProduction(env string) bool {
	switch strings.ToLower(strings.TrimSpace(env)) {
	case "local", "test", "dev", "development":
		return true
	default:
		return false
	}
}

func validPoint(p Point) bool {
	switch p {
	case PointBeforeClaim, PointAfterClaim, PointBeforeExecute, PointAfterExecute,
		PointBeforeResultPublish, PointAfterResultPublish:
		return true
	default:
		return false
	}
}

func validMode(m Mode) bool {
	switch m {
	case ModeError, ModeDelay, ModePanic:
		return true
	default:
		return false
	}
}

// New builds NoopInjector when disabled; ConfigurableInjector when enabled.
func New(cfg Config, opts ...Option) (Injector, error) {
	if !cfg.Enabled {
		return NoopInjector{}, nil
	}
	if err := cfg.validateEnabled(); err != nil {
		return nil, err
	}
	inj := &ConfigurableInjector{
		point: cfg.Point,
		mode:  cfg.Mode,
		count: cfg.Count,
		delay: cfg.Delay,
	}
	for _, opt := range opts {
		opt(inj)
	}
	return inj, nil
}

func (cfg Config) validateEnabled() error {
	if !validPoint(cfg.Point) {
		return fmt.Errorf("unsupported fault point %q", cfg.Point)
	}
	if !validMode(cfg.Mode) {
		return fmt.Errorf("unsupported fault mode %q", cfg.Mode)
	}
	if cfg.Count < 0 {
		return fmt.Errorf("fault count must be >= 0")
	}
	return nil
}

// Option configures a ConfigurableInjector.
type Option func(*ConfigurableInjector)

// WithObserver attaches observability hooks (log/metric/trace).
func WithObserver(obs Observer) Option {
	return func(inj *ConfigurableInjector) {
		inj.observer = obs
	}
}

// NoopInjector never injects faults (production default).
type NoopInjector struct{}

// Inject implements Injector.
func (NoopInjector) Inject(context.Context, Point) error { return nil }

// Observer receives fault-injection signals (no payloads).
type Observer interface {
	OnInject(ctx context.Context, point Point, mode Mode, remaining int)
}

// ConfigurableInjector triggers at most Count times at Point.
type ConfigurableInjector struct {
	mu       sync.Mutex
	point    Point
	mode     Mode
	count    int
	delay    time.Duration
	observer Observer
}

// Inject implements Injector.
func (inj *ConfigurableInjector) Inject(ctx context.Context, point Point) error {
	if inj == nil {
		return nil
	}
	inj.mu.Lock()
	if point != inj.point || inj.count <= 0 {
		inj.mu.Unlock()
		return nil
	}
	inj.count--
	remaining := inj.count
	mode := inj.mode
	delay := inj.delay
	obs := inj.observer
	inj.mu.Unlock()

	if obs != nil {
		obs.OnInject(ctx, point, mode, remaining)
	}

	switch mode {
	case ModeDelay:
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	case ModePanic:
		panic(fmt.Sprintf("kernelq injected panic at %s", point))
	default:
		return fmt.Errorf("%w at %s", ErrInjected, point)
	}
}

// Remaining returns how many injections are left (tests).
func (inj *ConfigurableInjector) Remaining() int {
	inj.mu.Lock()
	defer inj.mu.Unlock()
	return inj.count
}
