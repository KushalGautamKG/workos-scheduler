package worker

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// countingExecutor counts Execute calls for idempotency tests.
type countingExecutor struct {
	calls atomic.Int64
	err   error
}

func (executor *countingExecutor) Execute(task Task) (ExecutionResult, error) {
	executor.calls.Add(1)
	if executor.err != nil {
		return ExecutionResult{}, executor.err
	}
	return SuccessResult(), nil
}

// fakeClaimStore is an in-process IdempotencyStore for handler tests.
type fakeClaimStore struct {
	keys       map[string]struct{}
	forceError error
	lastKey    string
	lastTTL    time.Duration
}

func newFakeClaimStore() *fakeClaimStore {
	return &fakeClaimStore{keys: make(map[string]struct{})}
}

func (store *fakeClaimStore) TryClaim(key string, ttl time.Duration) (bool, error) {
	store.lastKey = key
	store.lastTTL = ttl
	if store.forceError != nil {
		return false, store.forceError
	}
	if _, exists := store.keys[key]; exists {
		return false, nil
	}
	store.keys[key] = struct{}{}
	return true, nil
}

func TestExecutionIdempotencyKeyFormat(t *testing.T) {
	key, err := ExecutionIdempotencyKey("job-abc", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "execution:job-abc:0" {
		t.Fatalf("key = %q, want execution:job-abc:0", key)
	}

	key, err = ExecutionIdempotencyKey("job-abc", 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if key != "execution:job-abc:3" {
		t.Fatalf("key = %q, want execution:job-abc:3", key)
	}
}

func TestExecutionIdempotencyKeyInvalidInput(t *testing.T) {
	if _, err := ExecutionIdempotencyKey("  ", 0); err == nil {
		t.Fatal("expected error for blank job id")
	}
	if _, err := ExecutionIdempotencyKey("job-a", -1); err == nil {
		t.Fatal("expected error for negative attempt")
	}
}

func TestHandleNoStoreConfiguredExecutorRunsNormally(t *testing.T) {
	executor := &countingExecutor{}
	handler := &DispatchEventHandler{Executor: executor}

	result, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != ExecutionSucceeded {
		t.Fatalf("status = %q, want %q", result.Status, ExecutionSucceeded)
	}
	if executor.calls.Load() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls.Load())
	}
}

func TestHandleFirstClaimExecutorRunsOnce(t *testing.T) {
	executor := &countingExecutor{}
	store := newFakeClaimStore()
	handler := &DispatchEventHandler{
		Executor:         executor,
		IdempotencyStore: store,
		IdempotencyTTL:   time.Hour,
	}

	result, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != ExecutionSucceeded {
		t.Fatalf("status = %q, want %q", result.Status, ExecutionSucceeded)
	}
	if executor.calls.Load() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls.Load())
	}
	if store.lastKey != "execution:job-123:0" {
		t.Fatalf("claimed key = %q, want execution:job-123:0", store.lastKey)
	}
}

func TestHandleDuplicateClaimExecutorNotCalled(t *testing.T) {
	executor := &countingExecutor{}
	store := newFakeClaimStore()
	handler := &DispatchEventHandler{
		Executor:         executor,
		IdempotencyStore: store,
	}
	event := validDispatchEventForHandler()

	first, err := handler.Handle(context.Background(), event)
	if err != nil || first.Status != ExecutionSucceeded {
		t.Fatalf("first: status=%q err=%v", first.Status, err)
	}

	second, err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("duplicate should not error: %v", err)
	}
	if second.Status != ExecutionDuplicateSkipped {
		t.Fatalf("status = %q, want %q", second.Status, ExecutionDuplicateSkipped)
	}
	if executor.calls.Load() != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls.Load())
	}
}

func TestHandleDuplicateResultMarkedSkippedNotFailed(t *testing.T) {
	handler := &DispatchEventHandler{
		Executor:         &countingExecutor{},
		IdempotencyStore: newFakeClaimStore(),
	}
	event := validDispatchEventForHandler()
	_, _ = handler.Handle(context.Background(), event)
	result, err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status == ExecutionRetryableFailure || result.Status == ExecutionTerminalFailure {
		t.Fatalf("duplicate must not be a failure, got %q", result.Status)
	}
	if result.Status != ExecutionDuplicateSkipped {
		t.Fatalf("status = %q, want %q", result.Status, ExecutionDuplicateSkipped)
	}
}

func TestHandleDuplicateCounterIncrements(t *testing.T) {
	handler := &DispatchEventHandler{
		Executor:         &countingExecutor{},
		IdempotencyStore: newFakeClaimStore(),
	}
	event := validDispatchEventForHandler()
	_, _ = handler.Handle(context.Background(), event)
	_, _ = handler.Handle(context.Background(), event)
	_, _ = handler.Handle(context.Background(), event)

	if handler.DuplicateExecutions() != 2 {
		t.Fatalf("duplicate_executions = %d, want 2", handler.DuplicateExecutions())
	}
	if handler.IdempotencyErrors() != 0 {
		t.Fatalf("idempotency_errors = %d, want 0", handler.IdempotencyErrors())
	}
}

func TestHandleStoreErrorExecutorNotCalled(t *testing.T) {
	executor := &countingExecutor{}
	store := newFakeClaimStore()
	store.forceError = errors.New("redis unavailable")
	handler := &DispatchEventHandler{
		Executor:         executor,
		IdempotencyStore: store,
	}

	_, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err == nil {
		t.Fatal("expected store error")
	}
	if !strings.Contains(err.Error(), "idempotency claim failed") {
		t.Fatalf("error = %v, want wrapped claim failure", err)
	}
	if executor.calls.Load() != 0 {
		t.Fatalf("executor must not run on store error, calls=%d", executor.calls.Load())
	}
}

func TestHandleStoreErrorIncrementsIdempotencyErrors(t *testing.T) {
	store := newFakeClaimStore()
	store.forceError = errors.New("redis unavailable")
	handler := &DispatchEventHandler{
		Executor:         &countingExecutor{},
		IdempotencyStore: store,
	}

	_, _ = handler.Handle(context.Background(), validDispatchEventForHandler())
	if handler.IdempotencyErrors() != 1 {
		t.Fatalf("idempotency_errors = %d, want 1", handler.IdempotencyErrors())
	}
	if handler.DuplicateExecutions() != 0 {
		t.Fatalf("duplicate_executions = %d, want 0", handler.DuplicateExecutions())
	}
}

func TestHandleDifferentAttemptsExecuteSeparately(t *testing.T) {
	executor := &countingExecutor{}
	handler := &DispatchEventHandler{
		Executor:         executor,
		IdempotencyStore: newFakeClaimStore(),
	}

	first := validDispatchEventForHandler()
	first.Attempt = 0
	second := validDispatchEventForHandler()
	second.Attempt = 1

	if _, err := handler.Handle(context.Background(), first); err != nil {
		t.Fatalf("attempt 0: %v", err)
	}
	if _, err := handler.Handle(context.Background(), second); err != nil {
		t.Fatalf("attempt 1: %v", err)
	}
	if executor.calls.Load() != 2 {
		t.Fatalf("executor calls = %d, want 2", executor.calls.Load())
	}
}

func TestHandleDifferentJobIDsExecuteSeparately(t *testing.T) {
	executor := &countingExecutor{}
	handler := &DispatchEventHandler{
		Executor:         executor,
		IdempotencyStore: newFakeClaimStore(),
	}

	a := validDispatchEventForHandler()
	a.JobID = "job-a"
	b := validDispatchEventForHandler()
	b.JobID = "job-b"

	if _, err := handler.Handle(context.Background(), a); err != nil {
		t.Fatalf("job-a: %v", err)
	}
	if _, err := handler.Handle(context.Background(), b); err != nil {
		t.Fatalf("job-b: %v", err)
	}
	if executor.calls.Load() != 2 {
		t.Fatalf("executor calls = %d, want 2", executor.calls.Load())
	}
}

func TestHandleDuplicateDoesNotPublishResult(t *testing.T) {
	producer := &fakeResultProducer{}
	handler := &DispatchEventHandler{
		Executor:         &countingExecutor{},
		ResultProducer:   producer,
		IdempotencyStore: newFakeClaimStore(),
	}
	event := validDispatchEventForHandler()
	_, _ = handler.Handle(context.Background(), event)
	_, err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(producer.Published) != 1 {
		t.Fatalf("published = %d, want 1 (no publish on duplicate)", len(producer.Published))
	}
}

func TestDuplicateSkippedResultValidates(t *testing.T) {
	result := DuplicateSkippedResult()
	if err := result.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if result.Status != ExecutionDuplicateSkipped {
		t.Fatalf("status = %q", result.Status)
	}
}
