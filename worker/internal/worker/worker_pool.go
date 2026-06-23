// Package worker holds types and logic for the KernelQ worker plane.
//
// This file implements a fixed-size goroutine pool that executes validated
// dispatch events concurrently. The Kafka poll loop decodes messages and
// enqueues work; pool workers call DispatchHandler.Handle.
package worker

import (
	"fmt"
	"sync"
)

// DefaultWorkerCount is how many goroutines run jobs when WorkerCount is unset.
const DefaultWorkerCount = 4

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
// There is no backpressure policy yet: the work channel is buffered so the
// poll loop can enqueue without blocking under normal local load.
type WorkerPool struct {
	workerCount int
	handler     DispatchHandler
	workCh      chan WorkItem
	wg          sync.WaitGroup

	onSuccess func(workerID string, item WorkItem)
	onError   func(workerID string, item WorkItem, err error)
}

// NewWorkerPool builds a pool that will call onSuccess/onError after each job.
//
// workerCount <= 0 uses DefaultWorkerCount (4).
func NewWorkerPool(
	workerCount int,
	handler DispatchHandler,
	onSuccess func(workerID string, item WorkItem),
	onError func(workerID string, item WorkItem, err error),
) *WorkerPool {
	if workerCount <= 0 {
		workerCount = DefaultWorkerCount
	}

	return &WorkerPool{
		workerCount: workerCount,
		handler:     handler,
		// Large buffer: concurrency only for now, not backpressure.
		workCh:      make(chan WorkItem, 1024),
		onSuccess:   onSuccess,
		onError:     onError,
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
func (pool *WorkerPool) Enqueue(item WorkItem) {
	pool.workCh <- item
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
