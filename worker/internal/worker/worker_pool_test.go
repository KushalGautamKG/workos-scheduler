package worker

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// blockingExecutor blocks in Execute until release receives a value.
// entered sends the job_id when execution starts (buffered for determinism).
type blockingExecutor struct {
	mu      sync.Mutex
	jobIDs  []string
	entered chan string
	release <-chan struct{}
}

func newBlockingExecutor(release <-chan struct{}, enteredBuffer int) *blockingExecutor {
	return &blockingExecutor{
		entered: make(chan string, enteredBuffer),
		release: release,
	}
}

func (executor *blockingExecutor) Execute(task Task) (ExecutionResult, error) {
	executor.entered <- task.JobID
	<-executor.release
	executor.mu.Lock()
	executor.jobIDs = append(executor.jobIDs, task.JobID)
	executor.mu.Unlock()
	return SuccessResult(), nil
}

func (executor *blockingExecutor) processedJobIDs() []string {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	out := make([]string, len(executor.jobIDs))
	copy(out, executor.jobIDs)
	return out
}

func workItemForJobID(jobID string) WorkItem {
	event := validDispatchEventForHandler()
	event.JobID = jobID
	return WorkItem{Event: event}
}

func TestWorkerPoolMultipleWorkersProcessJobs(t *testing.T) {
	release := make(chan struct{})
	executor := newBlockingExecutor(release, 3)
	handler := handlerWithExecutor(executor)

	var workerMu sync.Mutex
	workerIDs := make(map[string]struct{})

	pool := NewWorkerPool(3, 0, handler, func(workerID string, _ WorkItem) {
		workerMu.Lock()
		workerIDs[workerID] = struct{}{}
		workerMu.Unlock()
	}, nil)
	pool.Start()

	pool.Enqueue(workItemForJobID("job-1"))
	pool.Enqueue(workItemForJobID("job-2"))
	pool.Enqueue(workItemForJobID("job-3"))

	for index := 0; index < 3; index++ {
		select {
		case <-executor.entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for worker %d to start job", index+1)
		}
	}

	close(release)
	pool.Shutdown()

	workerMu.Lock()
	distinctWorkers := len(workerIDs)
	workerMu.Unlock()
	if distinctWorkers < 2 {
		t.Fatalf("expected multiple workers to process jobs, got %d distinct worker ids", distinctWorkers)
	}

	if len(executor.processedJobIDs()) != 3 {
		t.Fatalf("expected 3 processed jobs, got %d", len(executor.processedJobIDs()))
	}
}

func TestWorkerPoolProcessesAllSubmittedJobs(t *testing.T) {
	release := make(chan struct{})
	close(release)

	executor := newBlockingExecutor(release, 8)
	handler := handlerWithExecutor(executor)

	var processed int
	var processedMu sync.Mutex

	pool := NewWorkerPool(4, 0, handler, func(_ string, _ WorkItem) {
		processedMu.Lock()
		processed++
		processedMu.Unlock()
	}, nil)
	pool.Start()

	const jobCount = 8
	for index := 0; index < jobCount; index++ {
		pool.Enqueue(workItemForJobID(jobIDForIndex(index)))
	}
	pool.Shutdown()

	if processed != jobCount {
		t.Fatalf("expected %d processed jobs, got %d", jobCount, processed)
	}
	if len(executor.processedJobIDs()) != jobCount {
		t.Fatalf("expected executor to run %d jobs, got %d", jobCount, len(executor.processedJobIDs()))
	}
}

func TestWorkerPoolShutdownWaitsForWorkers(t *testing.T) {
	release := make(chan struct{})
	executor := newBlockingExecutor(release, 1)
	handler := handlerWithExecutor(executor)

	pool := NewWorkerPool(2, 0, handler, nil, nil)
	pool.Start()
	pool.Enqueue(workItemForJobID("job-blocking"))

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for in-flight job to start")
	}

	shutdownDone := make(chan struct{})
	go func() {
		pool.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before in-flight job completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for shutdown after job completed")
	}

	if len(executor.processedJobIDs()) != 1 {
		t.Fatalf("expected 1 processed job, got %d", len(executor.processedJobIDs()))
	}
}

func TestWorkerPoolRespectsWorkerCountConfiguration(t *testing.T) {
	if NewWorkerPool(0, 0, &countingHandler{}, nil, nil).workerCount != DefaultWorkerCount {
		t.Fatalf("expected default worker count %d", DefaultWorkerCount)
	}
	if NewWorkerPool(0, 0, &countingHandler{}, nil, nil).queueCapacity != DefaultQueueCapacity {
		t.Fatalf("expected default queue capacity %d", DefaultQueueCapacity)
	}

	const configured = 5
	pool := NewWorkerPool(configured, 0, &countingHandler{}, nil, nil)
	if pool.workerCount != configured {
		t.Fatalf("expected worker count %d, got %d", configured, pool.workerCount)
	}

	release := make(chan struct{})
	executor := newBlockingExecutor(release, configured)
	handler := handlerWithExecutor(executor)

	pool = NewWorkerPool(configured, 0, handler, nil, nil)
	pool.Start()

	for index := 0; index < configured; index++ {
		pool.Enqueue(workItemForJobID(jobIDForIndex(index)))
	}

	for index := 0; index < configured; index++ {
		select {
		case <-executor.entered:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for worker %d to start", index+1)
		}
	}

	close(release)
	pool.Shutdown()

	if len(executor.processedJobIDs()) != configured {
		t.Fatalf("expected %d processed jobs, got %d", configured, len(executor.processedJobIDs()))
	}
}

func TestWorkerPoolEnqueueReturnsErrorWhenQueueFull(t *testing.T) {
	release := make(chan struct{})
	executor := newBlockingExecutor(release, 2)
	handler := handlerWithExecutor(executor)

	pool := NewWorkerPool(1, 1, handler, nil, nil)
	pool.Start()

	if err := pool.Enqueue(workItemForJobID("job-1")); err != nil {
		t.Fatalf("expected first enqueue to succeed, got %v", err)
	}

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to start first job")
	}

	if err := pool.Enqueue(workItemForJobID("job-2")); err != nil {
		t.Fatalf("expected second enqueue to fill buffer, got %v", err)
	}

	err := pool.Enqueue(workItemForJobID("job-3"))
	if !errors.Is(err, ErrWorkerQueueFull) {
		t.Fatalf("expected ErrWorkerQueueFull, got %v", err)
	}

	close(release)
	pool.Shutdown()
}

func TestQueueDepthStartsAtZero(t *testing.T) {
	pool := NewWorkerPool(1, 3, &countingHandler{}, nil, nil)
	if depth := pool.QueueDepth(); depth != 0 {
		t.Fatalf("expected queue depth 0 before start, got %d", depth)
	}

	pool.Start()
	defer pool.Shutdown()

	if depth := pool.QueueDepth(); depth != 0 {
		t.Fatalf("expected queue depth 0 after start with no enqueues, got %d", depth)
	}
}

func TestQueueDepthIncreasesWhenWorkerBlocked(t *testing.T) {
	release := make(chan struct{})
	executor := newBlockingExecutor(release, 3)
	handler := handlerWithExecutor(executor)

	pool := NewWorkerPool(1, 3, handler, nil, nil)
	pool.Start()
	defer func() {
		close(release)
		pool.Shutdown()
	}()

	if err := pool.Enqueue(workItemForJobID("job-1")); err != nil {
		t.Fatalf("expected first enqueue to succeed, got %v", err)
	}

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to start first job")
	}

	if pool.QueueDepth() != 0 {
		t.Fatalf("expected queue depth 0 while worker holds first job, got %d", pool.QueueDepth())
	}

	if err := pool.Enqueue(workItemForJobID("job-2")); err != nil {
		t.Fatalf("expected second enqueue to succeed, got %v", err)
	}
	if err := pool.Enqueue(workItemForJobID("job-3")); err != nil {
		t.Fatalf("expected third enqueue to succeed, got %v", err)
	}

	if depth := pool.QueueDepth(); depth != 2 {
		t.Fatalf("expected queue depth 2 with worker blocked, got %d", depth)
	}
}

func TestQueueDepthDecreasesAfterWorkerDrains(t *testing.T) {
	release := make(chan struct{})
	executor := newBlockingExecutor(release, 2)
	handler := handlerWithExecutor(executor)

	var wg sync.WaitGroup
	wg.Add(2)

	pool := NewWorkerPool(1, 2, handler, func(_ string, _ WorkItem) {
		wg.Done()
	}, nil)
	pool.Start()
	defer pool.Shutdown()

	if err := pool.Enqueue(workItemForJobID("job-1")); err != nil {
		t.Fatalf("expected first enqueue to succeed, got %v", err)
	}

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to start first job")
	}

	if err := pool.Enqueue(workItemForJobID("job-2")); err != nil {
		t.Fatalf("expected second enqueue to succeed, got %v", err)
	}
	if depth := pool.QueueDepth(); depth != 1 {
		t.Fatalf("expected queue depth 1 before drain, got %d", depth)
	}

	close(release)

	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for workers to drain queued jobs")
	}

	if depth := pool.QueueDepth(); depth != 0 {
		t.Fatalf("expected queue depth 0 after drain, got %d", depth)
	}
}

func TestWorkerPoolRespectsQueueCapacityConfiguration(t *testing.T) {
	const configuredCapacity = 25
	pool := NewWorkerPool(2, configuredCapacity, &countingHandler{}, nil, nil)
	if pool.queueCapacity != configuredCapacity {
		t.Fatalf("expected queue capacity %d, got %d", configuredCapacity, pool.queueCapacity)
	}
	if cap(pool.workCh) != configuredCapacity {
		t.Fatalf("expected work channel capacity %d, got %d", configuredCapacity, cap(pool.workCh))
	}
}

func handlerWithExecutor(executor Executor) DispatchEventHandler {
	return DispatchEventHandler{
		Executor:   executor,
		WorkerName: "test-worker",
	}
}

func jobIDForIndex(index int) string {
	return fmt.Sprintf("job-%d", index)
}

// countingHandler is a minimal DispatchHandler for configuration tests.
type countingHandler struct {
	mu    sync.Mutex
	count int
}

func (handler *countingHandler) Handle(event DispatchEvent) (ExecutionResult, error) {
	handler.mu.Lock()
	handler.count++
	handler.mu.Unlock()
	return SuccessResult(), nil
}
