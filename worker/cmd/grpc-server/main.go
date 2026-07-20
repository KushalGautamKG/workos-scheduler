package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/config"
	workergrpc "github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc/pb"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"google.golang.org/grpc"
)

// loggingExecutor prints job ids for local smoke / loopback debugging.
type loggingExecutor struct {
	calls atomic.Int64
}

func (executor *loggingExecutor) Execute(task worker.Task) (worker.ExecutionResult, error) {
	executor.calls.Add(1)
	fmt.Printf("event=grpc_execute job_id=%s\n", task.JobID)
	return worker.SuccessResult(), nil
}

func main() {
	cfg, err := config.LoadGRPCConfig()
	if err != nil {
		log.Fatalf("load grpc config: %v", err)
	}

	otelCfg, err := telemetry.LoadConfig()
	if err != nil {
		log.Fatalf("load otel config: %v", err)
	}
	tracerProvider, err := telemetry.NewTracerProvider(context.Background(), otelCfg)
	if err != nil {
		log.Fatalf("otel tracer provider: %v", err)
	}
	fmt.Printf(
		"event=otel_tracer_provider_start enabled=%t exporter=%s service=%s\n",
		tracerProvider.Enabled(),
		otelCfg.Exporter,
		otelCfg.ServiceName,
	)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("event=otel_tracer_provider_shutdown error=%q\n", err.Error())
			return
		}
		fmt.Println("event=otel_tracer_provider_stopped")
	}()

	idempotencyStore, backend := buildIdempotencyStore()
	handler := &worker.DispatchEventHandler{
		Executor:         &loggingExecutor{},
		IdempotencyStore: idempotencyStore,
		IdempotencyTTL:   24 * time.Hour,
		WorkerName:       "kernelq-grpc-server",
	}

	health := workergrpc.NewHealth()

	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.Addr, err)
	}

	grpcServer := grpc.NewServer(telemetry.GRPCServerOptions()...)
	pb.RegisterWorkerExecutionServiceServer(grpcServer, &workergrpc.Server{
		Handler: handler,
	})
	health.Register(grpcServer)

	fmt.Printf(
		"event=grpc_server_start addr=%s idempotency_backend=%s shutdown_timeout=%s request_timeout=%s\n",
		cfg.Addr,
		backend,
		cfg.ShutdownTimeout,
		cfg.RequestTimeout,
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(listener)
	}()

	health.MarkReady()
	fmt.Println("event=grpc_server_ready status=SERVING")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		fmt.Println("event=grpc_server_shutdown reason=signal")
		health.MarkNotReady()
		fmt.Println("event=grpc_server_not_ready status=NOT_SERVING")

		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			fmt.Println("event=grpc_server_shutdown reason=graceful")
		case <-time.After(cfg.ShutdownTimeout):
			fmt.Println("event=grpc_server_shutdown reason=force_stop")
			grpcServer.Stop()
		}
	case err := <-errCh:
		health.MarkNotReady()
		if err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}

	fmt.Println("event=grpc_server_stopped")
}

// Default memory backend so loopback duplicate smoke works without Redis.
func buildIdempotencyStore() (worker.IdempotencyStore, string) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("KERNELQ_WORKER_IDEMPOTENCY_BACKEND")))
	switch backend {
	case "", "memory":
		return worker.NewInMemoryIdempotencyStore(), "memory"
	case "disabled", "off", "none":
		return nil, "disabled"
	default:
		fmt.Printf(
			"event=grpc_server_idempotency_fallback requested=%s using=memory\n",
			backend,
		)
		return worker.NewInMemoryIdempotencyStore(), "memory"
	}
}
