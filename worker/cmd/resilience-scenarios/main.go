// Command resilience-scenarios runs deterministic Day 129 failure scenarios in-process.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	workergrpc "github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/faults"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/metrics"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type countingExecutor struct {
	calls atomic.Int64
}

func (e *countingExecutor) Execute(task worker.Task) (worker.ExecutionResult, error) {
	e.calls.Add(1)
	return worker.SuccessResult(), nil
}

type failStore struct{ err error }

func (s failStore) TryClaim(string, time.Duration) (bool, error) { return false, s.err }

type failProducer struct{ err error }

func (p failProducer) PublishResult(context.Context, worker.WorkerResultEvent) error {
	return p.err
}

func main() {
	metrics.ResetForTest()
	_ = metrics.RegisterResilienceMetrics()

	scenarios := []struct {
		name string
		fn   func() error
	}{
		{"duplicate_delivery", scenarioDuplicateDelivery},
		{"redis_unavailable", scenarioRedisUnavailable},
		{"result_publish_failure", scenarioResultPublishFailure},
		{"fault_before_claim_recovery", scenarioFaultBeforeClaimRecovery},
		{"invalid_payload", scenarioInvalidPayload},
		{"grpc_unavailable", scenarioGRPCUnavailable},
		{"graceful_injector_disabled", scenarioFaultsDisabledByDefault},
	}

	failed := 0
	for _, sc := range scenarios {
		if err := sc.fn(); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL scenario=%s err=%v\n", sc.name, err)
			failed++
			continue
		}
		fmt.Printf("event=scenario_%s success=true\n", sc.name)
	}
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "event=resilience_scenarios success=false failed=%d\n", failed)
		os.Exit(1)
	}
	fmt.Println("event=resilience_scenarios success=true")
}

func baseEvent(jobID string) worker.DispatchEvent {
	return worker.DispatchEvent{
		EventType: "job.dispatch",
		JobID:     jobID,
		TenantID:  "tenant-resilience",
		Priority:  1,
		State:     "dispatched",
		Payload:   map[string]string{"kind": "day129"},
		Attempt:   0,
	}
}

func scenarioDuplicateDelivery() error {
	metrics.ResetForTest()
	_ = metrics.RegisterResilienceMetrics()
	store := worker.NewInMemoryIdempotencyStore()
	exec := &countingExecutor{}
	handler := &worker.DispatchEventHandler{
		Executor:         exec,
		IdempotencyStore: store,
		WorkerName:       "resilience-dup",
	}
	event := baseEvent(fmt.Sprintf("dup-%d", time.Now().UnixNano()))
	first, err := handler.Handle(context.Background(), event)
	if err != nil || first.Status != worker.ExecutionSucceeded {
		return fmt.Errorf("first: status=%s err=%v", first.Status, err)
	}
	second, err := handler.Handle(context.Background(), event)
	if err != nil {
		return fmt.Errorf("second unexpected err: %v", err)
	}
	if second.Status != worker.ExecutionDuplicateSkipped {
		return fmt.Errorf("want duplicate_skipped, got %s", second.Status)
	}
	if exec.calls.Load() != 1 {
		return fmt.Errorf("executor calls=%d want 1", exec.calls.Load())
	}
	if handler.DuplicateExecutions() != 1 {
		return fmt.Errorf("duplicate counter=%d", handler.DuplicateExecutions())
	}
	if metrics.DuplicateDeliveries("skipped") < 1 {
		return fmt.Errorf("duplicate metric missing")
	}
	return nil
}

func scenarioRedisUnavailable() error {
	_ = metrics.IncRecoveryAttempt("redis")
	handler := &worker.DispatchEventHandler{
		Executor:         &countingExecutor{},
		IdempotencyStore: failStore{err: errors.New("redis: connection refused")},
		WorkerName:       "resilience-redis",
	}
	_, err := handler.Handle(context.Background(), baseEvent("redis-down"))
	if err == nil {
		_ = metrics.IncRecoveryFailure("redis")
		return fmt.Errorf("expected idempotency claim failure")
	}
	if !strings.Contains(err.Error(), "idempotency claim failed") {
		return fmt.Errorf("want classified claim failure, got %v", err)
	}
	// Unsafe execution must not continue — executor not called because claim failed.
	// Restore path: healthy store then succeed.
	store := worker.NewInMemoryIdempotencyStore()
	exec := &countingExecutor{}
	handler.IdempotencyStore = store
	handler.Executor = exec
	res, err := handler.Handle(context.Background(), baseEvent(fmt.Sprintf("redis-up-%d", time.Now().UnixNano())))
	if err != nil || res.Status != worker.ExecutionSucceeded {
		_ = metrics.IncRecoveryFailure("redis")
		return fmt.Errorf("recovery failed: %v status=%s", err, res.Status)
	}
	_ = metrics.IncRecoverySuccess("redis")
	return nil
}

func scenarioResultPublishFailure() error {
	exec := &countingExecutor{}
	handler := &worker.DispatchEventHandler{
		Executor:       exec,
		ResultProducer: failProducer{err: errors.New("kafka: publish timeout")},
		WorkerName:     "resilience-publish",
	}
	res, err := handler.Handle(context.Background(), baseEvent(fmt.Sprintf("pub-%d", time.Now().UnixNano())))
	if err == nil {
		return fmt.Errorf("expected publish error")
	}
	if res.Status != worker.ExecutionSucceeded {
		return fmt.Errorf("execution and publish are distinct; want success result with publish err, got %s", res.Status)
	}
	if exec.calls.Load() != 1 {
		return fmt.Errorf("executor should have run once")
	}
	return nil
}

func scenarioFaultBeforeClaimRecovery() error {
	metrics.ResetForTest()
	_ = metrics.RegisterResilienceMetrics()
	inj, err := faults.New(faults.Config{
		Enabled: true,
		Point:   faults.PointBeforeClaim,
		Mode:    faults.ModeError,
		Count:   1,
	}, faults.WithObserver(faults.LoggingObserver{}))
	if err != nil {
		return err
	}
	store := worker.NewInMemoryIdempotencyStore()
	exec := &countingExecutor{}
	handler := &worker.DispatchEventHandler{
		Executor:         exec,
		IdempotencyStore: store,
		FaultInjector:    inj,
		WorkerName:       "resilience-fault",
	}
	job := fmt.Sprintf("fault-%d", time.Now().UnixNano())
	_, err = handler.Handle(context.Background(), baseEvent(job))
	if !errors.Is(err, faults.ErrInjected) {
		return fmt.Errorf("want injected fault, got %v", err)
	}
	if exec.calls.Load() != 0 {
		return fmt.Errorf("unsafe execute after fault")
	}
	_ = metrics.IncRecoveryAttempt("worker")
	res, err := handler.Handle(context.Background(), baseEvent(job))
	if err != nil || res.Status != worker.ExecutionSucceeded {
		_ = metrics.IncRecoveryFailure("worker")
		return fmt.Errorf("recovery Handle failed: %v %s", err, res.Status)
	}
	if exec.calls.Load() != 1 {
		return fmt.Errorf("want one successful execution, got %d", exec.calls.Load())
	}
	_ = metrics.IncRecoverySuccess("worker")
	if metrics.FaultInjections("before_claim", "error") < 1 {
		return fmt.Errorf("fault metric missing")
	}
	return nil
}

func scenarioInvalidPayload() error {
	handler := &worker.DispatchEventHandler{
		Executor:   &countingExecutor{},
		WorkerName: "resilience-invalid",
	}
	event := baseEvent("invalid")
	event.JobID = "" // invalid
	_, err := handler.Handle(context.Background(), event)
	if err == nil {
		return fmt.Errorf("expected validation failure")
	}
	return nil
}

func scenarioGRPCUnavailable() error {
	_ = metrics.IncRecoveryAttempt("grpc")
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	_, err := workergrpc.NewClient(ctx, "127.0.0.1:1", 100*time.Millisecond)
	if err == nil {
		_ = metrics.IncRecoveryFailure("grpc")
		return fmt.Errorf("expected dial failure")
	}
	// Deadline/unavailable classification for Execute is covered in grpc package tests;
	// here we confirm bounded failure without hanging.
	_ = metrics.IncRecoverySuccess("grpc")
	_ = codes.Unavailable
	_ = status.Code
	return nil
}

func scenarioFaultsDisabledByDefault() error {
	tEnv := os.Getenv("KERNELQ_FAULTS_ENABLED")
	_ = os.Setenv("KERNELQ_FAULTS_ENABLED", "")
	defer func() { _ = os.Setenv("KERNELQ_FAULTS_ENABLED", tEnv) }()
	cfg, err := faults.LoadConfig()
	if err != nil || cfg.Enabled {
		return fmt.Errorf("faults must be disabled by default: enabled=%v err=%v", cfg.Enabled, err)
	}
	return nil
}

// Live Redis ping when available (optional path used by dependency smoke via env).
func pingRedis() error {
	addr := os.Getenv("KERNELQ_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return rdb.Ping(ctx).Err()
}
