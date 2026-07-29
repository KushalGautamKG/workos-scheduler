// Package metrics provides a small in-process counter registry for KernelQ (Day 129).
//
// Counters are low-cardinality resilience signals. Labels must never include
// job_id, trace_id, or error_message.
package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	registryMu sync.Mutex
	counters   = map[string]*Counter{}
)

// Counter is a monotonically increasing, label-keyed series.
type Counter struct {
	name   string
	values sync.Map // labelKey -> *atomic.Int64
}

// RegisterCounter registers a named counter. Duplicate registration of the same
// name returns an error (fail-fast wiring).
func RegisterCounter(name string) (*Counter, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("metric name must be non-empty")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := counters[name]; exists {
		return nil, fmt.Errorf("metric %q already registered", name)
	}
	c := &Counter{name: name}
	counters[name] = c
	return c, nil
}

// ResetForTest clears the registry (unit tests only).
func ResetForTest() {
	registryMu.Lock()
	defer registryMu.Unlock()
	counters = map[string]*Counter{}
	faultInjections = nil
	recoveryAttempts = nil
	recoverySuccess = nil
	recoveryFailure = nil
	duplicateDeliveries = nil
	gracefulShutdownTimeout = nil
	resilienceRegistered.Store(false)
	registerErr = nil
}

// Inc increments by 1 with sorted label pairs (key,value,...).
func (c *Counter) Inc(labelPairs ...string) {
	c.Add(1, labelPairs...)
}

// Add increments by delta.
func (c *Counter) Add(delta int64, labelPairs ...string) {
	if c == nil || delta == 0 {
		return
	}
	key := labelsKey(labelPairs...)
	actual, _ := c.values.LoadOrStore(key, &atomic.Int64{})
	actual.(*atomic.Int64).Add(delta)
}

// Get returns the current value for labels.
func (c *Counter) Get(labelPairs ...string) int64 {
	if c == nil {
		return 0
	}
	key := labelsKey(labelPairs...)
	v, ok := c.values.Load(key)
	if !ok {
		return 0
	}
	return v.(*atomic.Int64).Load()
}

// Name returns the metric name.
func (c *Counter) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

func labelsKey(pairs ...string) string {
	if len(pairs) == 0 {
		return ""
	}
	if len(pairs)%2 != 0 {
		pairs = append(pairs, "")
	}
	type kv struct{ k, v string }
	items := make([]kv, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		items = append(items, kv{pairs[i], pairs[i+1]})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].k < items[j].k })
	var b strings.Builder
	for i, item := range items {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(item.k)
		b.WriteByte('=')
		b.WriteString(item.v)
	}
	return b.String()
}

// Well-known resilience metric names.
const (
	FaultInjectionsTotal         = "kernelq_fault_injections_total"
	RecoveryAttemptsTotal        = "kernelq_recovery_attempts_total"
	RecoverySuccessTotal         = "kernelq_recovery_success_total"
	RecoveryFailureTotal         = "kernelq_recovery_failure_total"
	DuplicateDeliveriesTotal     = "kernelq_duplicate_deliveries_total"
	GracefulShutdownTimeoutTotal = "kernelq_graceful_shutdown_timeout_total"
)

var (
	faultInjections         *Counter
	recoveryAttempts        *Counter
	recoverySuccess         *Counter
	recoveryFailure         *Counter
	duplicateDeliveries     *Counter
	gracefulShutdownTimeout *Counter
	resilienceRegistered    atomic.Bool
	registerErr             error
)

// RegisterResilienceMetrics registers Day 129 counters once per process/test reset.
func RegisterResilienceMetrics() error {
	if resilienceRegistered.Load() {
		return registerErr
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if resilienceRegistered.Load() {
		return registerErr
	}

	var err error
	if faultInjections, err = registerLocked(FaultInjectionsTotal); err != nil {
		registerErr = err
		return err
	}
	if recoveryAttempts, err = registerLocked(RecoveryAttemptsTotal); err != nil {
		registerErr = err
		return err
	}
	if recoverySuccess, err = registerLocked(RecoverySuccessTotal); err != nil {
		registerErr = err
		return err
	}
	if recoveryFailure, err = registerLocked(RecoveryFailureTotal); err != nil {
		registerErr = err
		return err
	}
	if duplicateDeliveries, err = registerLocked(DuplicateDeliveriesTotal); err != nil {
		registerErr = err
		return err
	}
	if gracefulShutdownTimeout, err = registerLocked(GracefulShutdownTimeoutTotal); err != nil {
		registerErr = err
		return err
	}
	resilienceRegistered.Store(true)
	registerErr = nil
	return nil
}

func registerLocked(name string) (*Counter, error) {
	if _, exists := counters[name]; exists {
		return nil, fmt.Errorf("metric %q already registered", name)
	}
	c := &Counter{name: name}
	counters[name] = c
	return c, nil
}

// IncFaultInjection increments fault injection counter.
func IncFaultInjection(faultPoint, faultMode string) error {
	if err := RegisterResilienceMetrics(); err != nil {
		return err
	}
	faultInjections.Inc("fault_point", faultPoint, "fault_mode", faultMode)
	return nil
}

// IncDuplicateDelivery increments duplicate delivery counter.
func IncDuplicateDelivery(outcome string) error {
	if err := RegisterResilienceMetrics(); err != nil {
		return err
	}
	duplicateDeliveries.Inc("outcome", outcome)
	return nil
}

// IncRecoveryAttempt increments recovery attempt counter.
func IncRecoveryAttempt(dependency string) error {
	if err := RegisterResilienceMetrics(); err != nil {
		return err
	}
	recoveryAttempts.Inc("dependency", dependency)
	return nil
}

// IncRecoverySuccess increments recovery success counter.
func IncRecoverySuccess(dependency string) error {
	if err := RegisterResilienceMetrics(); err != nil {
		return err
	}
	recoverySuccess.Inc("dependency", dependency, "outcome", "success")
	return nil
}

// IncRecoveryFailure increments recovery failure counter.
func IncRecoveryFailure(dependency string) error {
	if err := RegisterResilienceMetrics(); err != nil {
		return err
	}
	recoveryFailure.Inc("dependency", dependency, "outcome", "failure")
	return nil
}

// IncGracefulShutdownTimeout increments shutdown timeout counter.
func IncGracefulShutdownTimeout() error {
	if err := RegisterResilienceMetrics(); err != nil {
		return err
	}
	gracefulShutdownTimeout.Inc()
	return nil
}

// FaultInjections returns current count for tests.
func FaultInjections(faultPoint, faultMode string) int64 {
	_ = RegisterResilienceMetrics()
	if faultInjections == nil {
		return 0
	}
	return faultInjections.Get("fault_point", faultPoint, "fault_mode", faultMode)
}

// DuplicateDeliveries returns current count for tests.
func DuplicateDeliveries(outcome string) int64 {
	_ = RegisterResilienceMetrics()
	if duplicateDeliveries == nil {
		return 0
	}
	return duplicateDeliveries.Get("outcome", outcome)
}
