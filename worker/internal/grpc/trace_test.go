package grpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc/pb"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type successTraceHandler struct{}

func (successTraceHandler) Handle(ctx context.Context, event worker.DispatchEvent) (worker.ExecutionResult, error) {
	_ = ctx
	_ = event
	return worker.SuccessResult(), nil
}

func setupTraceExporter(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})
	return exporter
}

func startTraceLoopback(t *testing.T, handler ExecutionHandler) (addr string, stop func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(telemetry.GRPCServerOptions()...)
	pb.RegisterWorkerExecutionServiceServer(server, &Server{Handler: handler})
	errCh := make(chan error, 1)
	go func() { errCh <- server.Serve(listener) }()
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

func dialTraceClient(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	opts = append(opts, telemetry.GRPCDialOptions()...)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestGRPCTracePropagationParentChild(t *testing.T) {
	exporter := setupTraceExporter(t)
	handler := &worker.DispatchEventHandler{
		Executor: successExecutor{},
	}
	addr, stop := startTraceLoopback(t, handler)
	defer stop()

	conn := dialTraceClient(t, addr)
	client := pb.NewWorkerExecutionServiceClient(conn)

	rootCtx, rootSpan := otel.Tracer("test").Start(context.Background(), "test.root")
	resp, err := client.Execute(rootCtx, &pb.ExecuteRequest{JobId: "job-propagate", Attempt: 1})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if resp.GetStatus() != StatusSuccess {
		t.Fatalf("status = %q", resp.GetStatus())
	}
	rootSpan.End()

	spans := exporter.GetSpans()
	if len(spans) < 3 {
		t.Fatalf("expected at least 3 spans, got %d: %v", len(spans), spanNames(spans))
	}

	var clientSpan, serverSpan, execSpan *tracetest.SpanStub
	for i := range spans {
		s := &spans[i]
		switch s.SpanKind {
		case trace.SpanKindClient:
			if clientSpan == nil {
				clientSpan = s
			}
		case trace.SpanKindServer:
			if serverSpan == nil {
				serverSpan = s
			}
		}
		if s.Name == telemetry.SpanWorkerExecute {
			execSpan = s
		}
	}
	if clientSpan == nil || serverSpan == nil || execSpan == nil {
		t.Fatalf("missing spans among %v", spanNames(spans))
	}

	rootID := rootSpan.SpanContext().TraceID()
	if clientSpan.SpanContext.TraceID() != rootID ||
		serverSpan.SpanContext.TraceID() != rootID ||
		execSpan.SpanContext.TraceID() != rootID {
		t.Fatalf("spans not on same trace as root %s", rootID)
	}

	if !hasAncestor(spans, execSpan, serverSpan.SpanContext.SpanID()) {
		t.Fatalf(
			"worker.execute not under server span (hierarchy=%v)",
			describeHierarchy(spans),
		)
	}
	for _, s := range spans {
		if s.EndTime.IsZero() {
			t.Fatalf("span %q missing EndTime", s.Name)
		}
	}
}

func TestGRPCTracePropagationOnExecutionError(t *testing.T) {
	exporter := setupTraceExporter(t)
	handler := &worker.DispatchEventHandler{
		Executor: failingExecutor{},
	}
	addr, stop := startTraceLoopback(t, handler)
	defer stop()

	conn := dialTraceClient(t, addr)
	client := pb.NewWorkerExecutionServiceClient(conn)

	rootCtx, rootSpan := otel.Tracer("test").Start(context.Background(), "test.root.error")
	resp, err := client.Execute(rootCtx, &pb.ExecuteRequest{JobId: "job-fail", Attempt: 0})
	if err != nil {
		t.Fatalf("unexpected RPC transport error: %v", err)
	}
	if resp.GetStatus() != StatusFailed {
		t.Fatalf("status = %q, want FAILED", resp.GetStatus())
	}
	rootSpan.End()

	spans := exporter.GetSpans()
	traceID := rootSpan.SpanContext().TraceID()
	var sawClient, sawServer, sawExec bool
	for _, s := range spans {
		if s.SpanContext.TraceID() != traceID {
			continue
		}
		switch s.SpanKind {
		case trace.SpanKindClient:
			sawClient = true
		case trace.SpanKindServer:
			sawServer = true
		}
		if s.Name == telemetry.SpanWorkerExecute {
			sawExec = true
			foundFailed := false
			for _, a := range s.Attributes {
				if string(a.Key) == telemetry.AttrExecutionStatus &&
					a.Value.AsString() == telemetry.ExecutionStatusFailed {
					foundFailed = true
				}
			}
			if !foundFailed {
				t.Fatalf("worker.execute missing failed status")
			}
			if len(s.Events) == 0 {
				t.Fatal("worker.execute should RecordError on failure")
			}
		}
		if s.EndTime.IsZero() {
			t.Fatalf("span %q not ended", s.Name)
		}
	}
	if !sawClient || !sawServer || !sawExec {
		t.Fatalf("expected client+server+execute; got %v", spanNames(spans))
	}
}

type successExecutor struct{}

func (successExecutor) Execute(task worker.Task) (worker.ExecutionResult, error) {
	_ = task
	return worker.SuccessResult(), nil
}

type failingExecutor struct{}

func (failingExecutor) Execute(task worker.Task) (worker.ExecutionResult, error) {
	_ = task
	return worker.ExecutionResult{}, errors.New("forced execution failure")
}

func TestGRPCValidationErrorStillEndsRPCSpans(t *testing.T) {
	exporter := setupTraceExporter(t)
	addr, stop := startTraceLoopback(t, successTraceHandler{})
	defer stop()

	conn := dialTraceClient(t, addr)
	client := pb.NewWorkerExecutionServiceClient(conn)

	_, err := client.Execute(context.Background(), &pb.ExecuteRequest{JobId: "", Attempt: 0})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code = %v, want InvalidArgument", status.Code(err))
	}

	spans := exporter.GetSpans()
	var sawClient, sawServer, sawExec bool
	for _, s := range spans {
		switch s.SpanKind {
		case trace.SpanKindClient:
			sawClient = true
		case trace.SpanKindServer:
			sawServer = true
		}
		if s.Name == telemetry.SpanWorkerExecute {
			sawExec = true
		}
		if s.EndTime.IsZero() {
			t.Fatalf("span %q not ended", s.Name)
		}
	}
	if !sawClient || !sawServer {
		t.Fatalf("expected RPC spans; got %v", spanNames(spans))
	}
	if sawExec {
		t.Fatal("worker.execute should not run for invalid request")
	}
}

func spanNames(spans tracetest.SpanStubs) []string {
	names := make([]string, 0, len(spans))
	for _, s := range spans {
		names = append(names, s.Name)
	}
	return names
}

func describeHierarchy(spans tracetest.SpanStubs) map[string]string {
	out := make(map[string]string, len(spans))
	for _, s := range spans {
		out[s.Name] = s.Parent.SpanID().String()
	}
	return out
}

func hasAncestor(spans tracetest.SpanStubs, child *tracetest.SpanStub, ancestor trace.SpanID) bool {
	byID := make(map[trace.SpanID]*tracetest.SpanStub, len(spans))
	for i := range spans {
		byID[spans[i].SpanContext.SpanID()] = &spans[i]
	}
	current := child.Parent.SpanID()
	for hops := 0; hops < len(spans)+1; hops++ {
		if !current.IsValid() {
			return false
		}
		if current == ancestor {
			return true
		}
		parent, ok := byID[current]
		if !ok {
			return false
		}
		current = parent.Parent.SpanID()
	}
	return false
}
