// Package worker holds types and logic for the KernelQ worker plane.
//
// This file implements a fixed-size goroutine pool that executes validated
// dispatch events concurrently. The Kafka poll loop decodes messages and
// enqueues work; pool workers call DispatchHandler.Handle.
package worker

import (
	"errors"
	"fmt"
	"sync"
)

// DefaultWorkerCount is how many goroutines run jobs when WorkerCount is unset.
const DefaultWorkerCount = 4

// DefaultQueueCapacity is the bounded work-queue size when QueueCapacity is unset.
//
// A bounded queue caps how many decoded jobs wait in memory between the Kafka
// consumer and pool workers. When the queue fills, Enqueue returns
// ErrWorkerQueueFull instead of blocking — the first backpressure boundary.
const DefaultQueueCapacity = 100

// ErrWorkerQueueFull is returned when the bounded work queue has no free slots.
var ErrWorkerQueueFull = errors.New("worker queue full")

// WorkItem is one decoded dispatch job waiting for a pool worker.
//
// Original Kafka fields are kept so execution failures can still publish
// dead-letter events with the same metadata as the synchronous path.
type WorkItem struct {
	Event         DispatchEvent
	OriginalKey   string
	OriginalValue []byte
	SourceTopic   string
}

// WorkerPool runs DispatchHandler.Handle on a fixed number of goroutines.
//
// The internal work channel is buffered to queueCapacity. That bounds memory
// for waiting jobs and makes queue depth observable when saturated.
type WorkerPool struct {
	workerCount   int
	queueCapacity int
	handler       DispatchHandler
	workCh        chan WorkItem
	wg            sync.WaitGroup

	onSuccess func(workerID string, item WorkItem)
	onError   func(workerID string, item WorkItem, err error)
}

// NewWorkerPool builds a pool that will call onSuccess/onError after each job.
//
// workerCount <= 0 uses DefaultWorkerCount (4).
// queueCapacity <= 0 uses DefaultQueueCapacity (100).
func NewWorkerPool(
	workerCount int,
	queueCapacity int,
	handler DispatchHandler,
	onSuccess func(workerID string, item WorkItem),
	onError func(workerID string, item WorkItem, err error),
) *WorkerPool {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}
	if queueCapacity <= 0 {
		queueCapacity = DefaultQueueCapacity
	}

	return &WorkerPool{
		workerCount:   workerCount,
		queueCapacity: queueCapacity,
		handler:       handler,
		workCh:        make(chan WorkItem, queueCapacity),
		onSuccess:     onSuccess,
		onError:       onError,
	}
}

// Start launches worker goroutines. Each worker has a stable id (worker-1, …).
func (pool *WorkerPool) Start() {
	for index := 0; index < pool.workerCount; index++ {
		workerID := fmt.Sprintf("worker-%d", index+1)
		pool.wg.Add(1)
		go pool.runWorker(workerID)
	}
}

// Enqueue adds one decoded job for a pool worker to execute.
//
// Uses a non-blocking send: when the bounded queue is full it returns
// ErrWorkerQueueFull immediately so the poll loop is not stuck. Kafka
// pause/resume and retry topics are future work; callers record saturation
// via stats until then.
func (pool *WorkerPool) Enqueue(item WorkItem) error {
	select {
	case pool.workCh <- item:
		return nil
	default:
		return ErrWorkerQueueFull
	}
}

// Shutdown closes the work channel and waits for all workers to finish.
func (pool *WorkerPool) Shutdown() {
	close(pool.workCh)
	pool.wg.Wait()
}

func (pool *WorkerPool) runWorker(workerID string) {
	defer pool.wg.Done()

	for item := range pool.workCh {
		_, err := pool.handler.Handle(item.Event)
		if err != nil {
			if pool.onError != nil {
				pool.onError(workerID, item, err)
			}
			continue
		}
		if pool.onSuccess != nil {
			pool.onSuccess(workerID, item)
		}
	}
}
