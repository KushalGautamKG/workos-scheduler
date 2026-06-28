package worker

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestConsumerStatsIncludesWorkQueueDepth(t *testing.T) {
	restoreQueueFullRetrySleep(t)
	sleepQueueFullRetry = func(time.Duration) {}

	consumer, _, _, _ := saturatedPoolForQueueFullTests(t)

	if consumer.Stats.WorkQueueDepth != 1 {
		t.Fatalf("expected WorkQueueDepth 1 with one buffered job, got %d", consumer.Stats.WorkQueueDepth)
	}
}

func TestRunRecordsWorkQueueDepthOnShutdown(t *testing.T) {
	release := make(chan struct{})
	executor := newBlockingExecutor(release, 4)
	handler := handlerWithExecutor(executor)

	poller := &fakePoller{
		events: []kafka.Event{
			newKafkaMessage("job-1", validDispatchJSON()),
			newKafkaMessage("job-2", validDispatchJSON()),
		},
	}
	consumer := &KafkaConsumer{
		Poller:        poller,
		Runner:        ConsumerRunner{Handler: handler},
		WorkerCount:   1,
		QueueCapacity: 3,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- consumer.Run(ctx, 100)
	}()

	for poller.index < 2 {
		runtime.Gosched()
	}

	select {
	case <-executor.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for worker to block on first job")
	}

	deadline := time.Now().Add(2 * time.Second)
	for consumer.Stats.WorkQueueDepth != 1 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if consumer.Stats.WorkQueueDepth != 1 {
		t.Fatalf("expected WorkQueueDepth 1 with one buffered job, got %d", consumer.Stats.WorkQueueDepth)
	}

	cancel()

	go func() {
		close(release)
	}()

	if err := <-done; err != nil {
		t.Fatalf("expected nil on cancel, got error: %v", err)
	}
}

func TestConsumerShutdownOutputIncludesWorkQueueDepth(t *testing.T) {
	lines := ConsumerShutdownStatsLines(ConsumerStats{
		MessagesSeen:             10,
		MessagesProcessed:        8,
		MessageErrors:            1,
		KafkaErrors:              0,
		WorkQueueCapacity:        100,
		WorkQueueDepth:           2,
		WorkItemsEnqueued:        9,
		WorkQueueFullErrors:      1,
		BackpressurePauseEvents:  2,
		BackpressureResumeEvents: 1,
	})

	foundDepth := false
	foundPause := false
	foundResume := false
	for _, line := range lines {
		switch line {
		case "work_queue_depth=2":
			foundDepth = true
		case "backpressure_pause_events=2":
			foundPause = true
		case "backpressure_resume_events=1":
			foundResume = true
		}
	}
	if !foundDepth {
		t.Fatalf("expected work_queue_depth line in shutdown stats, got %v", lines)
	}
	if !foundPause {
		t.Fatalf("expected backpressure_pause_events line in shutdown stats, got %v", lines)
	}
	if !foundResume {
		t.Fatalf("expected backpressure_resume_events line in shutdown stats, got %v", lines)
	}
}
