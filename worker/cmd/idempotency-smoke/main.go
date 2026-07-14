package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"github.com/redis/go-redis/v9"
)

// Live Redis smoke: DispatchEventHandler execution idempotency (Day 113).
// Same job_id + attempt twice → executor runs once; second is duplicate_skipped.
// Run via worker/scripts/smoke_worker_execution_idempotency.sh — no Kafka.
type countingExecutor struct {
	calls atomic.Int64
}

func (executor *countingExecutor) Execute(task worker.Task) (worker.ExecutionResult, error) {
	executor.calls.Add(1)
	return worker.SuccessResult(), nil
}

func main() {
	addr := envOr("KERNELQ_REDIS_ADDR", "localhost:6379")
	namespace := envOr("KERNELQ_REDIS_NAMESPACE", "kernelq:idempotency")
	jobID := fmt.Sprintf("day113-execution-idem-%d", time.Now().UnixNano())
	attempt := 0

	logicalKey, err := worker.ExecutionIdempotencyKey(jobID, attempt)
	if err != nil {
		fail(err.Error())
	}
	redisKey := namespace + ":" + logicalKey

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fail(fmt.Sprintf("redis ping failed: %v", err))
	}

	// Clean slate so a prior smoke run cannot make the first claim look like a duplicate.
	if err := rdb.Del(ctx, redisKey).Err(); err != nil {
		fail(fmt.Sprintf("delete key before smoke: %v", err))
	}
	defer func() {
		_ = rdb.Del(context.Background(), redisKey).Err()
	}()

	adapter, err := worker.NewGoRedisSetNXClient(rdb)
	if err != nil {
		fail(err.Error())
	}
	store, err := worker.NewRedisIdempotencyStore(adapter, namespace)
	if err != nil {
		fail(err.Error())
	}

	executor := &countingExecutor{}
	handler := &worker.DispatchEventHandler{
		Executor:         executor,
		IdempotencyStore: store,
		IdempotencyTTL:   24 * time.Hour,
		WorkerName:       "day113-smoke",
	}

	event := worker.DispatchEvent{
		EventType: "job.dispatch",
		JobID:     jobID,
		TenantID:  "tenant-smoke",
		Priority:  1,
		State:     "dispatched",
		Payload:   map[string]string{"kind": "day113-smoke"},
		Attempt:   attempt,
	}

	first, err := handler.Handle(event)
	if err != nil {
		fail(fmt.Sprintf("first handle: %v", err))
	}
	second, err := handler.Handle(event)
	if err != nil {
		fail(fmt.Sprintf("second handle: %v", err))
	}

	executorCalls := executor.calls.Load()
	duplicates := handler.DuplicateExecutions()
	idempotencyErrors := handler.IdempotencyErrors()
	firstExecuted := first.Status == worker.ExecutionSucceeded
	secondSkipped := second.Status == worker.ExecutionDuplicateSkipped

	fmt.Printf("executor_calls=%d\n", executorCalls)
	fmt.Printf("duplicate_executions=%d\n", duplicates)
	fmt.Printf("idempotency_errors=%d\n", idempotencyErrors)
	fmt.Printf("first_executed=%t\n", firstExecuted)
	fmt.Printf("second_skipped=%t\n", secondSkipped)

	if executorCalls != 1 || duplicates != 1 || idempotencyErrors != 0 || !firstExecuted || !secondSkipped {
		fail(fmt.Sprintf(
			"assertions failed: calls=%d dup=%d errs=%d first=%t second=%t",
			executorCalls, duplicates, idempotencyErrors, firstExecuted, secondSkipped,
		))
	}

	fmt.Println("PASS: worker execution idempotency smoke test succeeded")
	fmt.Println("event=smoke_worker_execution_idempotency success=true")
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func fail(message string) {
	fmt.Fprintf(os.Stderr, "FAIL: %s\n", message)
	fmt.Println("event=smoke_worker_execution_idempotency success=false")
	os.Exit(1)
}
