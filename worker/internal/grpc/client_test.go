package grpc

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc/pb"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type slowExecutionHandler struct {
	delay  time.Duration
	result worker.ExecutionResult
}

func (h *slowExecutionHandler) Handle(ctx context.Context, event worker.DispatchEvent) (worker.ExecutionResult, error) {
	time.Sleep(h.delay)
	return h.result, nil
}

func startLoopbackServer(t *testing.T, handler ExecutionHandler) (addr string, stop func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	server := grpc.NewServer(telemetry.GRPCServerOptions()...)
	pb.RegisterWorkerExecutionServiceServer(server, &Server{Handler: handler})

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.Serve(listener)
	}()

	stop = func() {
		server.GracefulStop()
		_ = listener.Close()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
		}
	}

	return listener.Addr().String(), stop
}

func TestClientExecuteSuccess(t *testing.T) {
	handler := &fakeExecutionHandler{result: worker.SuccessResult()}
	addr, stop := startLoopbackServer(t, handler)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := NewClient(ctx, addr, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	resp, err := client.Execute(ctx, "job-ok", 0, map[string]string{"kind": "loopback"})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.GetStatus() != StatusSuccess {
		t.Fatalf("status = %q, want %s", resp.GetStatus(), StatusSuccess)
	}
	if resp.GetDuplicateSkipped() {
		t.Fatal("duplicate_skipped should be false")
	}
	if handler.called != 1 {
		t.Fatalf("handler called %d times, want 1", handler.called)
	}
	if handler.lastEvent.JobID != "job-ok" {
		t.Fatalf("job_id = %q", handler.lastEvent.JobID)
	}
}

func TestClientExecuteValidationFailure(t *testing.T) {
	handler := &fakeExecutionHandler{result: worker.SuccessResult()}
	addr, stop := startLoopbackServer(t, handler)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := NewClient(ctx, addr, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	_, err = client.Execute(ctx, "", 0, nil)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}
	if handler.called != 0 {
		t.Fatalf("handler should not run, called=%d", handler.called)
	}
}

func TestClientExecuteDuplicateMapping(t *testing.T) {
	handler := &fakeExecutionHandler{result: worker.DuplicateSkippedResult()}
	addr, stop := startLoopbackServer(t, handler)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client, err := NewClient(ctx, addr, time.Second)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	resp, err := client.Execute(ctx, "job-dup", 1, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.GetStatus() != StatusDuplicateSkipped {
		t.Fatalf("status = %q, want %s", resp.GetStatus(), StatusDuplicateSkipped)
	}
	if !resp.GetDuplicateSkipped() {
		t.Fatal("duplicate_skipped should be true")
	}
}

func TestClientServerUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, err := NewClient(ctx, "127.0.0.1:1", 200*time.Millisecond)
	if err == nil {
		t.Fatal("expected dial error for unavailable server")
	}
	if !strings.Contains(err.Error(), "dial") {
		t.Fatalf("error should mention dial, got %v", err)
	}
}

func TestClientTimeoutPropagation(t *testing.T) {
	handler := &slowExecutionHandler{
		delay:  400 * time.Millisecond,
		result: worker.SuccessResult(),
	}
	addr, stop := startLoopbackServer(t, handler)
	defer stop()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := NewClient(ctx, addr, 50*time.Millisecond)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	_, err = client.Execute(ctx, "job-slow", 0, nil)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	code := status.Code(err)
	if code != codes.DeadlineExceeded && code != codes.Canceled {
		// Some stacks wrap the deadline as a client-side context error.
		if !strings.Contains(err.Error(), "DeadlineExceeded") &&
			!strings.Contains(err.Error(), "context deadline exceeded") {
			t.Fatalf("want deadline/cancel, got code=%v err=%v", code, err)
		}
	}
}
