package worker

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func restoreQueueFullRetrySleep(t *testing.T) {
	t.Helper()
	original := sleepQueueFullRetry
	t.Cleanup(func() {
		sleepQueueFullRetry = original
	})
}

// saturatedPoolForQueueFullTests returns a blocked pool (worker_count=1, queue_capacity=1)
// with job-1 running and job-2 waiting in the buffer.
func saturatedPoolForQueueFullTests(t *testing.T) (*KafkaConsumer, *WorkerPool, *blockingExecutor, chan struct{}) {
	t.Helper()

	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() {
		releaseOnce.Do(func() { close(release) })
	}

	executor := newBlockingExecutor(release, 4)
	handler := handlerWithExecutor(executor)
	consumer := &KafkaConsumer{
		Runner: ConsumerRunner{Handler: handler},
	}
	pool := NewWorkerPool(1, 1, handler, nil, nil)
	pool.Start()
	t.Cleanup(func() {
		closeRelease()
		pool.Shutdown()
	})

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-1", validDispatchJSON()))

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to block on first job")
	}

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-2", validDispatchJSON()))
	return consumer, pool, executor, release
}

func TestQueueFullTriggersBackoff(t *testing.T) {
	restoreQueueFullRetrySleep(t)

	backoffObserved := make(chan struct{}, 1)
	sleepQueueFullRetry = func(time.Duration) {
		backoffObserved <- struct{}{}
	}

	consumer, pool, _, _ := saturatedPoolForQueueFullTests(t)
	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-3", validDispatchJSON()))

	select {
	case <-backoffObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("expected queue-full backoff before retry")
	}
}

func TestQueueFullRetrySucceedsAfterQueueFrees(t *testing.T) {
	restoreQueueFullRetrySleep(t)

	backoffStarted := make(chan struct{}, 1)
	backoffRelease := make(chan struct{})
	sleepQueueFullRetry = func(time.Duration) {
		backoffStarted <- struct{}{}
		<-backoffRelease
	}

	release := make(chan struct{})
	var releaseOnce sync.Once
	closeRelease := func() {
		releaseOnce.Do(func() { close(release) })
	}

	executor := newBlockingExecutor(release, 3)
	handler := handlerWithExecutor(executor)
	consumer := &KafkaConsumer{
		Runner: ConsumerRunner{Handler: handler},
	}
	pool := NewWorkerPool(1, 1, handler, nil, nil)
	pool.Start()
	t.Cleanup(func() {
		closeRelease()
		pool.Shutdown()
	})

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-1", validDispatchJSON()))

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to block on first job")
	}

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-2", validDispatchJSON()))

	done := make(chan struct{})
	go func() {
		consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-3", validDispatchJSON()))
		close(done)
	}()

	select {
	case <-backoffStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for queue-full backoff")
	}

	closeRelease()

	deadline := time.Now().Add(2 * time.Second)
	for len(executor.processedJobIDs()) < 2 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if len(executor.processedJobIDs()) < 2 {
		t.Fatal("timed out waiting for in-flight jobs to finish")
	}

	backoffRelease <- struct{}{}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for enqueue with retry")
	}

	if consumer.Stats.WorkQueueFullErrors != 1 {
		t.Fatalf("expected WorkQueueFullErrors 1, got %d", consumer.Stats.WorkQueueFullErrors)
	}
	if consumer.Stats.WorkItemsEnqueued != 3 {
		t.Fatalf("expected WorkItemsEnqueued 3 after retry, got %d", consumer.Stats.WorkItemsEnqueued)
	}
}

func TestQueueFullRetryFailurePreservesDroppedBehavior(t *testing.T) {
	restoreQueueFullRetrySleep(t)

	sleepQueueFullRetry = func(time.Duration) {}

	consumer, pool, _, _ := saturatedPoolForQueueFullTests(t)
	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-3", validDispatchJSON()))

	if consumer.Stats.MessagesSeen != 3 {
		t.Fatalf("expected MessagesSeen 3, got %d", consumer.Stats.MessagesSeen)
	}
	if consumer.Stats.WorkItemsEnqueued != 2 {
		t.Fatalf("expected WorkItemsEnqueued 2, got %d", consumer.Stats.WorkItemsEnqueued)
	}
	if consumer.Stats.WorkQueueFullErrors != 1 {
		t.Fatalf("expected WorkQueueFullErrors 1, got %d", consumer.Stats.WorkQueueFullErrors)
	}
	if consumer.Stats.MessageErrors != 0 {
		t.Fatalf("expected MessageErrors 0, got %d", consumer.Stats.MessageErrors)
	}
	if consumer.Stats.MessagesProcessed != 0 {
		t.Fatalf("expected MessagesProcessed 0 while worker blocked, got %d", consumer.Stats.MessagesProcessed)
	}
}

func TestQueueFullCounterIncrementsOncePerMessage(t *testing.T) {
	restoreQueueFullRetrySleep(t)

	sleepQueueFullRetry = func(time.Duration) {}

	consumer, pool, _, _ := saturatedPoolForQueueFullTests(t)

	before := consumer.Stats.WorkQueueFullErrors
	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-3", validDispatchJSON()))
	afterOneDrop := consumer.Stats.WorkQueueFullErrors
	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-4", validDispatchJSON()))
	afterTwoDrops := consumer.Stats.WorkQueueFullErrors

	if afterOneDrop-before != 1 {
		t.Fatalf("expected 1 queue-full error for first dropped message, got %d", afterOneDrop-before)
	}
	if afterTwoDrops-afterOneDrop != 1 {
		t.Fatalf("expected 1 queue-full error for second dropped message, got %d", afterTwoDrops-afterOneDrop)
	}
	if afterTwoDrops-before != 2 {
		t.Fatalf("expected 2 total queue-full errors, got %d", afterTwoDrops-before)
	}
}
