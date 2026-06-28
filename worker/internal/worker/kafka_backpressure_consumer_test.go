package worker

import (
	"sync"
	"testing"
	"time"
)

// recordingPauseResumeController wraps InMemoryPauseResumeController and counts
// Pause/Resume invocations for deterministic policy wiring tests.
type recordingPauseResumeController struct {
	*InMemoryPauseResumeController
	recordsMu       sync.Mutex
	pauseCallCount  int
	resumeCallCount int
}

func newRecordingPauseResumeController() *recordingPauseResumeController {
	return &recordingPauseResumeController{
		InMemoryPauseResumeController: NewInMemoryPauseResumeController(),
	}
}

func (controller *recordingPauseResumeController) Pause() error {
	controller.recordsMu.Lock()
	controller.pauseCallCount++
	controller.recordsMu.Unlock()
	return controller.InMemoryPauseResumeController.Pause()
}

func (controller *recordingPauseResumeController) Resume() error {
	controller.recordsMu.Lock()
	controller.resumeCallCount++
	controller.recordsMu.Unlock()
	return controller.InMemoryPauseResumeController.Resume()
}

func (controller *recordingPauseResumeController) pauseCalls() int {
	controller.recordsMu.Lock()
	defer controller.recordsMu.Unlock()
	return controller.pauseCallCount
}

func (controller *recordingPauseResumeController) resumeCalls() int {
	controller.recordsMu.Lock()
	defer controller.recordsMu.Unlock()
	return controller.resumeCallCount
}

func newBackpressureConsumer(
	policy BackpressurePolicy,
	controller PauseResumeController,
) *KafkaConsumer {
	return &KafkaConsumer{
		BackpressurePolicy:    &policy,
		PauseResumeController: controller,
	}
}

// blockedPoolAtDepth returns a pool with one worker blocked on the first job and
// bufferDepth jobs waiting in the bounded queue (channel occupancy only).
func blockedPoolAtDepth(
	t *testing.T,
	capacity int,
	bufferDepth int,
) (*WorkerPool, chan struct{}) {
	t.Helper()

	release := make(chan struct{})
	executor := newBlockingExecutor(release, 1+bufferDepth)
	handler := handlerWithExecutor(executor)

	pool := NewWorkerPool(1, capacity, handler, nil, nil)
	pool.Start()
	t.Cleanup(func() {
		close(release)
		pool.Shutdown()
	})

	if err := pool.Enqueue(workItemForJobID("job-running")); err != nil {
		t.Fatalf("enqueue running job: %v", err)
	}

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to block on first job")
	}

	for index := 0; index < bufferDepth; index++ {
		if err := pool.Enqueue(workItemForJobID(jobIDForIndex(index))); err != nil {
			t.Fatalf("enqueue buffered job %d: %v", index, err)
		}
	}

	if depth := pool.QueueDepth(); depth != bufferDepth {
		t.Fatalf("expected queue depth %d, got %d", bufferDepth, depth)
	}

	return pool, release
}

func TestBackpressurePauseCalledAtHighWatermark(t *testing.T) {
	// capacity 5, high watermark ceil(5*0.80)=4
	pool, _ := blockedPoolAtDepth(t, 5, 4)

	controller := newRecordingPauseResumeController()
	consumer := newBackpressureConsumer(defaultBackpressurePolicy(), controller)

	consumer.maybeApplyBackpressure(pool)

	if !controller.IsPaused() {
		t.Fatal("expected controller paused at high watermark")
	}
	if controller.pauseCalls() != 1 {
		t.Fatalf("expected Pause called once, got %d", controller.pauseCalls())
	}
	if controller.resumeCalls() != 0 {
		t.Fatalf("expected no Resume calls, got %d", controller.resumeCalls())
	}
}

func TestBackpressurePauseNotCalledRepeatedlyWhilePaused(t *testing.T) {
	pool, _ := blockedPoolAtDepth(t, 5, 4)

	controller := newRecordingPauseResumeController()
	consumer := newBackpressureConsumer(defaultBackpressurePolicy(), controller)

	consumer.maybeApplyBackpressure(pool)
	consumer.maybeApplyBackpressure(pool)
	consumer.maybeApplyBackpressure(pool)

	if controller.pauseCalls() != 1 {
		t.Fatalf("expected Pause called once while already paused, got %d", controller.pauseCalls())
	}
	if !controller.IsPaused() {
		t.Fatal("expected controller to remain paused")
	}
}

func TestBackpressureResumeCalledBelowLowWatermark(t *testing.T) {
	// capacity 10, high watermark 8 — pause at depth 8; low watermark 5.
	pool, _ := blockedPoolAtDepth(t, 10, 8)

	controller := newRecordingPauseResumeController()
	consumer := newBackpressureConsumer(defaultBackpressurePolicy(), controller)

	consumer.maybeApplyBackpressure(pool)
	if !controller.IsPaused() {
		t.Fatalf("expected paused at high watermark, depth=%d", pool.QueueDepth())
	}
	if controller.pauseCalls() != 1 {
		t.Fatalf("expected one Pause before resume test, got %d", controller.pauseCalls())
	}

	// Simulate drain: empty pool at depth 0 (below low watermark floor(10*0.50)=5).
	emptyPool := NewWorkerPool(1, 10, &countingHandler{}, nil, nil)
	emptyPool.Start()
	t.Cleanup(emptyPool.Shutdown)

	consumer.maybeApplyBackpressure(emptyPool)

	if controller.IsPaused() {
		t.Fatal("expected controller resumed at depth 0")
	}
	if controller.resumeCalls() != 1 {
		t.Fatalf("expected Resume called once, got %d", controller.resumeCalls())
	}
}

func TestBackpressureNoOpWithoutPolicyOrController(t *testing.T) {
	pool, _ := blockedPoolAtDepth(t, 5, 4)

	controller := newRecordingPauseResumeController()
	policy := defaultBackpressurePolicy()

	cases := []struct {
		name     string
		consumer *KafkaConsumer
	}{
		{
			name:     "both unset",
			consumer: &KafkaConsumer{},
		},
		{
			name:     "policy only",
			consumer: &KafkaConsumer{BackpressurePolicy: &policy},
		},
		{
			name: "controller only",
			consumer: &KafkaConsumer{
				PauseResumeController: controller,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			controller.recordsMu.Lock()
			controller.pauseCallCount = 0
			controller.resumeCallCount = 0
			controller.recordsMu.Unlock()
			_ = controller.InMemoryPauseResumeController.Resume()

			tc.consumer.maybeApplyBackpressure(pool)

			if controller.pauseCalls() != 0 {
				t.Fatalf("expected no Pause calls, got %d", controller.pauseCalls())
			}
			if controller.resumeCalls() != 0 {
				t.Fatalf("expected no Resume calls, got %d", controller.resumeCalls())
			}
			if controller.IsPaused() {
				t.Fatal("expected controller to remain unpaused")
			}
		})
	}
}

func TestBackpressureStatsIncrementOnPauseAndResume(t *testing.T) {
	pool, _ := blockedPoolAtDepth(t, 5, 4)

	controller := newRecordingPauseResumeController()
	consumer := newBackpressureConsumer(defaultBackpressurePolicy(), controller)

	consumer.maybeApplyBackpressure(pool)
	if consumer.Stats.BackpressurePauseEvents != 1 {
		t.Fatalf("expected BackpressurePauseEvents 1 after pause, got %d", consumer.Stats.BackpressurePauseEvents)
	}
	if consumer.Stats.BackpressureResumeEvents != 0 {
		t.Fatalf("expected BackpressureResumeEvents 0 after pause, got %d", consumer.Stats.BackpressureResumeEvents)
	}

	emptyPool := NewWorkerPool(1, 10, &countingHandler{}, nil, nil)
	emptyPool.Start()
	t.Cleanup(emptyPool.Shutdown)

	consumer.maybeApplyBackpressure(emptyPool)
	if consumer.Stats.BackpressurePauseEvents != 1 {
		t.Fatalf("expected BackpressurePauseEvents unchanged at 1, got %d", consumer.Stats.BackpressurePauseEvents)
	}
	if consumer.Stats.BackpressureResumeEvents != 1 {
		t.Fatalf("expected BackpressureResumeEvents 1 after resume, got %d", consumer.Stats.BackpressureResumeEvents)
	}
}
