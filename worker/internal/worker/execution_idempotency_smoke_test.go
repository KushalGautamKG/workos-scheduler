package worker

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis-gated integration: DispatchEventHandler + RedisIdempotencyStore.
// Skips cleanly when Redis is unavailable. No Kafka.
func TestExecutionIdempotencyLiveRedisHandlerSkipsDuplicate(t *testing.T) {
	addr := os.Getenv("KERNELQ_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	namespace := os.Getenv("KERNELQ_REDIS_NAMESPACE")
	if namespace == "" {
		namespace = "kernelq:idempotency"
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis unavailable at %s: %v", addr, err)
	}

	jobID := fmt.Sprintf("day113-test-%d", time.Now().UnixNano())
	logicalKey, err := ExecutionIdempotencyKey(jobID, 0)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	redisKey := namespace + ":" + logicalKey

	if err := rdb.Del(ctx, redisKey).Err(); err != nil {
		t.Fatalf("delete before: %v", err)
	}
	t.Cleanup(func() {
		_ = rdb.Del(context.Background(), redisKey).Err()
	})

	adapter, err := NewGoRedisSetNXClient(rdb)
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	store, err := NewRedisIdempotencyStore(adapter, namespace)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	var calls atomic.Int64
	executor := ExecutorFunc(func(task Task) (ExecutionResult, error) {
		calls.Add(1)
		return SuccessResult(), nil
	})

	handler := &DispatchEventHandler{
		Executor:         executor,
		IdempotencyStore: store,
		IdempotencyTTL:   time.Hour,
	}

	event := DispatchEvent{
		EventType: "job.dispatch",
		JobID:     jobID,
		TenantID:  "tenant-test",
		Priority:  1,
		State:     "dispatched",
		Payload:   map[string]string{"kind": "day113"},
		Attempt:   0,
	}

	first, err := handler.Handle(event)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	if first.Status != ExecutionSucceeded {
		t.Fatalf("first status = %q, want %q", first.Status, ExecutionSucceeded)
	}

	second, err := handler.Handle(event)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second.Status != ExecutionDuplicateSkipped {
		t.Fatalf("second status = %q, want %q", second.Status, ExecutionDuplicateSkipped)
	}

	if calls.Load() != 1 {
		t.Fatalf("executor calls = %d, want 1", calls.Load())
	}
	if handler.DuplicateExecutions() != 1 {
		t.Fatalf("duplicate_executions = %d, want 1", handler.DuplicateExecutions())
	}
	if handler.IdempotencyErrors() != 0 {
		t.Fatalf("idempotency_errors = %d, want 0", handler.IdempotencyErrors())
	}
}

// ExecutorFunc adapts a function to the Executor interface for tests.
type ExecutorFunc func(Task) (ExecutionResult, error)

func (fn ExecutorFunc) Execute(task Task) (ExecutionResult, error) {
	return fn(task)
}
