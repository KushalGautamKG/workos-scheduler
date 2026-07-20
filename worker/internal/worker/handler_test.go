package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// fakeExecutor records the Task passed to Execute and returns test-controlled
// outcomes. Set result for business outcomes; set err for infrastructure failures.
type fakeExecutor struct {
	received Task
	result   ExecutionResult
	err      error
}

func (executor *fakeExecutor) Execute(task Task) (ExecutionResult, error) {
	executor.received = task

	if executor.err != nil {
		return ExecutionResult{}, executor.err
	}

	// Default to success when the test did not configure a specific outcome.
	if executor.result.Status == "" {
		return SuccessResult(), nil
	}

	return executor.result, nil
}

func validDispatchEventForHandler() DispatchEvent {
	return DispatchEvent{
		EventType: "job.dispatch",
		JobID:     "job-123",
		TenantID:  "tenant-a",
		Priority:  5,
		State:     "dispatched",
		Payload:   map[string]string{"kind": "day35-smoke"},
	}
}

// fakeResultProducer records published events and can simulate publish failures.
type fakeResultProducer struct {
	Published []WorkerResultEvent
	err       error
}

func (producer *fakeResultProducer) PublishResult(event WorkerResultEvent) error {
	if producer.err != nil {
		return producer.err
	}
	if err := event.Validate(); err != nil {
		return err
	}
	producer.Published = append(producer.Published, event)
	return nil
}

func TestHandleSuccessResultFlowsThroughHandler(t *testing.T) {
	executor := &fakeExecutor{result: SuccessResult()}
	handler := DispatchEventHandler{Executor: executor}
	event := validDispatchEventForHandler()

	result, err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if result.Status != ExecutionSucceeded {
		t.Fatalf("expected status %q, got %q", ExecutionSucceeded, result.Status)
	}

	// Handler should map the dispatch event onto a Task before calling Execute.
	if executor.received.JobID != event.JobID {
		t.Fatalf("expected job id %q, got %q", event.JobID, executor.received.JobID)
	}
	if executor.received.TenantID != event.TenantID {
		t.Fatalf("expected tenant id %q, got %q", event.TenantID, executor.received.TenantID)
	}
	if executor.received.Priority != event.Priority {
		t.Fatalf("expected priority %d, got %d", event.Priority, executor.received.Priority)
	}
	if executor.received.Payload["kind"] != "day35-smoke" {
		t.Fatalf("expected payload kind day35-smoke, got %v", executor.received.Payload)
	}
}

func TestHandleRetryableFailureResultFlowsThroughHandler(t *testing.T) {
	expected := RetryableFailureResult("dependency timeout")
	executor := &fakeExecutor{result: expected}
	handler := DispatchEventHandler{Executor: executor}

	result, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("expected retryable failure to flow through, got error: %v", err)
	}
	if result.Status != ExecutionRetryableFailure {
		t.Fatalf("expected status %q, got %q", ExecutionRetryableFailure, result.Status)
	}
	if result.Message != expected.Message {
		t.Fatalf("expected message %q, got %q", expected.Message, result.Message)
	}
}

func TestHandleTerminalFailureResultFlowsThroughHandler(t *testing.T) {
	expected := TerminalFailureResult("invalid payload in task")
	executor := &fakeExecutor{result: expected}
	handler := DispatchEventHandler{Executor: executor}

	result, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("expected terminal failure to flow through, got error: %v", err)
	}
	if result.Status != ExecutionTerminalFailure {
		t.Fatalf("expected status %q, got %q", ExecutionTerminalFailure, result.Status)
	}
	if result.Message != expected.Message {
		t.Fatalf("expected message %q, got %q", expected.Message, result.Message)
	}
}

func TestHandleNilExecutorReturnsError(t *testing.T) {
	handler := DispatchEventHandler{Executor: nil}

	_, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err == nil {
		t.Fatal("expected error when executor is nil, got nil")
	}
	if !strings.Contains(err.Error(), "executor") {
		t.Fatalf("expected executor error, got: %v", err)
	}
}

func TestHandleInvalidTaskReturnsError(t *testing.T) {
	executor := &fakeExecutor{}
	handler := DispatchEventHandler{Executor: executor}

	event := validDispatchEventForHandler()
	event.JobID = "   "

	_, err := handler.Handle(context.Background(), event)
	if err == nil {
		t.Fatal("expected validation error for blank job id, got nil")
	}
	if !strings.Contains(err.Error(), "job id") {
		t.Fatalf("expected job id validation error, got: %v", err)
	}
}

func TestHandleExecutorInfrastructureErrorReturnsError(t *testing.T) {
	expectedErr := errors.New("postgres unreachable")
	executor := &fakeExecutor{err: expectedErr}
	handler := DispatchEventHandler{Executor: executor}

	_, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err == nil {
		t.Fatal("expected infrastructure error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
}

func TestHandleInvalidExecutionResultReturnsError(t *testing.T) {
	executor := &fakeExecutor{
		result: ExecutionResult{
			Status:  ExecutionStatus("unknown"),
			Message: "bad executor outcome",
		},
	}
	handler := DispatchEventHandler{Executor: executor}

	_, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err == nil {
		t.Fatal("expected error for invalid execution result, got nil")
	}
	if !strings.Contains(err.Error(), "status") {
		t.Fatalf("expected status validation error, got: %v", err)
	}
}

func TestHandleSuccessfulExecutionPublishesWorkerResultEvent(t *testing.T) {
	producer := &RecordingResultProducer{}
	handler := DispatchEventHandler{
		Executor:       &fakeExecutor{result: SuccessResult()},
		ResultProducer: producer,
	}
	event := validDispatchEventForHandler()

	result, err := handler.Handle(context.Background(), event)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if result.Status != ExecutionSucceeded {
		t.Fatalf("expected status %q, got %q", ExecutionSucceeded, result.Status)
	}
	if len(producer.Published) != 1 {
		t.Fatalf("expected 1 published result event, got %d", len(producer.Published))
	}
	if producer.Published[0].JobID != event.JobID {
		t.Fatalf("expected job_id %q, got %q", event.JobID, producer.Published[0].JobID)
	}
	if producer.Published[0].Status != ExecutionSucceeded {
		t.Fatalf("expected published status %q, got %q", ExecutionSucceeded, producer.Published[0].Status)
	}
}

func TestHandleRetryableFailurePublishesWorkerResultEvent(t *testing.T) {
	expected := RetryableFailureResult("dependency timeout")
	producer := &RecordingResultProducer{}
	handler := DispatchEventHandler{
		Executor:       &fakeExecutor{result: expected},
		ResultProducer: producer,
	}

	_, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if len(producer.Published) != 1 {
		t.Fatalf("expected 1 published result event, got %d", len(producer.Published))
	}
	if producer.Published[0].Status != ExecutionRetryableFailure {
		t.Fatalf("expected status %q, got %q", ExecutionRetryableFailure, producer.Published[0].Status)
	}
	if producer.Published[0].Message != "dependency timeout" {
		t.Fatalf("expected message %q, got %q", "dependency timeout", producer.Published[0].Message)
	}
}

func TestHandleTerminalFailurePublishesWorkerResultEvent(t *testing.T) {
	expected := TerminalFailureResult("max retries exhausted")
	producer := &RecordingResultProducer{}
	handler := DispatchEventHandler{
		Executor:       &fakeExecutor{result: expected},
		ResultProducer: producer,
	}

	_, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if len(producer.Published) != 1 {
		t.Fatalf("expected 1 published result event, got %d", len(producer.Published))
	}
	if producer.Published[0].Status != ExecutionTerminalFailure {
		t.Fatalf("expected status %q, got %q", ExecutionTerminalFailure, producer.Published[0].Status)
	}
}

func TestHandlePublishesWorkerResultEventWithCustomWorkerName(t *testing.T) {
	producer := &RecordingResultProducer{}
	handler := DispatchEventHandler{
		Executor:       &fakeExecutor{result: SuccessResult()},
		ResultProducer: producer,
		WorkerName:     "custom-worker-1",
	}

	_, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if producer.Published[0].Worker != "custom-worker-1" {
		t.Fatalf("expected worker %q, got %q", "custom-worker-1", producer.Published[0].Worker)
	}
}

func TestHandleDefaultsBlankWorkerNameToKernelqGoWorker(t *testing.T) {
	producer := &RecordingResultProducer{}
	handler := DispatchEventHandler{
		Executor:       &fakeExecutor{result: SuccessResult()},
		ResultProducer: producer,
		WorkerName:     "   ",
	}

	_, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if producer.Published[0].Worker != workerIdentity {
		t.Fatalf("expected worker %q, got %q", workerIdentity, producer.Published[0].Worker)
	}
}

func TestHandleReturnsResultProducerPublishError(t *testing.T) {
	expectedErr := errors.New("simulated result publish failure")
	expectedResult := SuccessResult()
	producer := &fakeResultProducer{err: expectedErr}
	handler := DispatchEventHandler{
		Executor:       &fakeExecutor{result: expectedResult},
		ResultProducer: producer,
	}

	result, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err == nil {
		t.Fatal("expected publish error, got nil")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected %v, got %v", expectedErr, err)
	}
	if result.Status != ExecutionSucceeded {
		t.Fatalf("expected result status %q, got %q", ExecutionSucceeded, result.Status)
	}
	if len(producer.Published) != 0 {
		t.Fatalf("expected no published events, got %d", len(producer.Published))
	}
}

func TestHandleReturnsExecutionResultWhenResultProducerNil(t *testing.T) {
	handler := DispatchEventHandler{
		Executor:       &fakeExecutor{result: SuccessResult()},
		ResultProducer: nil,
	}

	result, err := handler.Handle(context.Background(), validDispatchEventForHandler())
	if err != nil {
		t.Fatalf("expected success without producer, got error: %v", err)
	}
	if result.Status != ExecutionSucceeded {
		t.Fatalf("expected status %q, got %q", ExecutionSucceeded, result.Status)
	}
}
