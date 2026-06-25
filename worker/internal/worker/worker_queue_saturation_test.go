package worker

import (
	"fmt"
	"testing"
	"time"
)

// TestWorkerQueueSaturationStats exercises the consumer enqueue path under a
// blocked executor. No real Kafka — pool capacity 1, one worker, barrier sync.
func TestWorkerQueueSaturationStats(t *testing.T) {
	const (
		workerCount   = 1
		queueCapacity = 1
		extraJobs     = 3
	)

	release := make(chan struct{})
	executor := newBlockingExecutor(release, 1+extraJobs)
	handler := handlerWithExecutor(executor)
	consumer := &KafkaConsumer{
		Runner: ConsumerRunner{Handler: handler},
	}

	pool := NewWorkerPool(workerCount, queueCapacity, handler, nil, nil)
	consumer.recordWorkQueueCapacity(pool.QueueCapacity())
	pool.Start()
	defer func() {
		close(release)
		pool.Shutdown()
	}()

	consumer.enqueueKafkaMessage(pool, newKafkaMessage("job-1", validDispatchJSON()))

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to block on first job")
	}

	for index := 2; index <= 1+extraJobs; index++ {
		key := fmt.Sprintf("job-%d", index)
		consumer.enqueueKafkaMessage(pool, newKafkaMessage(key, validDispatchJSON()))
	}

	if consumer.Stats.WorkQueueCapacity != queueCapacity {
		t.Fatalf("expected WorkQueueCapacity %d, got %d", queueCapacity, consumer.Stats.WorkQueueCapacity)
	}
	if consumer.Stats.WorkItemsEnqueued <= 0 {
		t.Fatalf("expected WorkItemsEnqueued > 0, got %d", consumer.Stats.WorkItemsEnqueued)
	}
	if consumer.Stats.WorkQueueFullErrors <= 0 {
		t.Fatalf("expected WorkQueueFullErrors > 0, got %d", consumer.Stats.WorkQueueFullErrors)
	}
	if consumer.Stats.MessageErrors != 0 {
		t.Fatalf("expected MessageErrors 0 (saturation is not a decode/handler error), got %d", consumer.Stats.MessageErrors)
	}
}
