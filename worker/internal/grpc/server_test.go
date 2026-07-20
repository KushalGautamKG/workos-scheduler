package grpc

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc/pb"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeExecutionHandler struct {
	lastEvent worker.DispatchEvent
	called    int
	result    worker.ExecutionResult
	err       error
}

func (f *fakeExecutionHandler) Handle(ctx context.Context, event worker.DispatchEvent) (worker.ExecutionResult, error) {
	f.called++
	f.lastEvent = event
	return f.result, f.err
}

func TestExecuteRejectsEmptyJobID(t *testing.T) {
	handler := &fakeExecutionHandler{result: worker.SuccessResult()}
	server := &Server{Handler: handler}

	_, err := server.Execute(context.Background(), &pb.ExecuteRequest{
		JobId:   "   ",
		Attempt: 0,
	})
	if err == nil {
		t.Fatal("expected validation error for empty job_id")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
	if handler.called != 0 {
		t.Fatalf("handler should not be called, got %d", handler.called)
	}
}

func TestExecuteRejectsNegativeAttempt(t *testing.T) {
	handler := &fakeExecutionHandler{result: worker.SuccessResult()}
	server := &Server{Handler: handler}

	_, err := server.Execute(context.Background(), &pb.ExecuteRequest{
		JobId:   "job-1",
		Attempt: -1,
	})
	if err == nil {
		t.Fatal("expected validation error for negative attempt")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
	if !strings.Contains(err.Error(), "attempt") {
		t.Fatalf("error should mention attempt, got %v", err)
	}
	if handler.called != 0 {
		t.Fatalf("handler should not be called, got %d", handler.called)
	}
}

func TestExecuteRejectsNilRequest(t *testing.T) {
	server := &Server{Handler: &fakeExecutionHandler{result: worker.SuccessResult()}}
	_, err := server.Execute(context.Background(), nil)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestExecuteUnimplementedWhenHandlerNil(t *testing.T) {
	server := &Server{}
	_, err := server.Execute(context.Background(), &pb.ExecuteRequest{
		JobId:   "job-1",
		Attempt: 0,
	})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("code = %v, want Unimplemented", status.Code(err))
	}
}

func TestExecuteInvokesHandler(t *testing.T) {
	handler := &fakeExecutionHandler{result: worker.SuccessResult()}
	server := &Server{Handler: handler}

	resp, err := server.Execute(context.Background(), &pb.ExecuteRequest{
		JobId:   "job-42",
		Attempt: 3,
		Payload: map[string]string{"kind": "unit"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handler.called != 1 {
		t.Fatalf("handler called %d times, want 1", handler.called)
	}
	if handler.lastEvent.JobID != "job-42" {
		t.Fatalf("job_id = %q, want job-42", handler.lastEvent.JobID)
	}
	if handler.lastEvent.Attempt != 3 {
		t.Fatalf("attempt = %d, want 3", handler.lastEvent.Attempt)
	}
	if handler.lastEvent.Payload["kind"] != "unit" {
		t.Fatalf("payload = %#v", handler.lastEvent.Payload)
	}
	if resp.GetStatus() != StatusSuccess {
		t.Fatalf("status = %q, want %s", resp.GetStatus(), StatusSuccess)
	}
}

func TestExecuteMapsSuccess(t *testing.T) {
	server := &Server{Handler: &fakeExecutionHandler{result: worker.SuccessResult()}}
	resp, err := server.Execute(context.Background(), &pb.ExecuteRequest{
		JobId:   "job-ok",
		Attempt: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStatus() != StatusSuccess {
		t.Fatalf("status = %q, want %s", resp.GetStatus(), StatusSuccess)
	}
	if resp.GetDuplicateSkipped() {
		t.Fatal("duplicate_skipped should be false")
	}
}

func TestExecuteMapsDuplicateSkipped(t *testing.T) {
	server := &Server{
		Handler: &fakeExecutionHandler{result: worker.DuplicateSkippedResult()},
	}
	resp, err := server.Execute(context.Background(), &pb.ExecuteRequest{
		JobId:   "job-dup",
		Attempt: 0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStatus() != StatusDuplicateSkipped {
		t.Fatalf("status = %q, want %s", resp.GetStatus(), StatusDuplicateSkipped)
	}
	if !resp.GetDuplicateSkipped() {
		t.Fatal("duplicate_skipped should be true")
	}
}

func TestExecuteMapsFailure(t *testing.T) {
	server := &Server{
		Handler: &fakeExecutionHandler{
			result: worker.RetryableFailureResult("boom"),
		},
	}
	resp, err := server.Execute(context.Background(), &pb.ExecuteRequest{
		JobId:   "job-fail",
		Attempt: 1,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStatus() != StatusFailed {
		t.Fatalf("status = %q, want %s", resp.GetStatus(), StatusFailed)
	}
	if resp.GetErrorMessage() != "boom" {
		t.Fatalf("error_message = %q, want boom", resp.GetErrorMessage())
	}
	if resp.GetDuplicateSkipped() {
		t.Fatal("duplicate_skipped should be false")
	}
}

func TestExecuteMapsHandlerErrorAsFailed(t *testing.T) {
	server := &Server{
		Handler: &fakeExecutionHandler{err: fmt.Errorf("infra down")},
	}
	resp, err := server.Execute(context.Background(), &pb.ExecuteRequest{
		JobId:   "job-err",
		Attempt: 0,
	})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if resp.GetStatus() != StatusFailed {
		t.Fatalf("status = %q, want %s", resp.GetStatus(), StatusFailed)
	}
	if !strings.Contains(resp.GetErrorMessage(), "infra down") {
		t.Fatalf("error_message = %q", resp.GetErrorMessage())
	}
}
