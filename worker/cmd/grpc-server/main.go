package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
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
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/logging"
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
	return worker.SuccessResult(), nil
}

func main() {
	logCfg, err := logging.LoadConfig()
	if err != nil {
		log.Fatalf("load logging config: %v", err)
	}
	logger, err := logging.New(logCfg, os.Stdout)
	if err != nil {
		log.Fatalf("create logger: %v", err)
	}
	slog.SetDefault(logger)
	logger.Info("worker starting", "component", "worker", "operation", "lifecycle", "status", "starting")

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
	logger.Info(
		"otel tracer provider start",
		"component", "telemetry",
		"operation", "lifecycle",
		"status", "started",
		"otel_enabled", tracerProvider.Enabled(),
		"otel_exporter", otelCfg.Exporter,
	)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
			logger.Error(
				"otel tracer provider shutdown",
				"component", "telemetry",
				"operation", "lifecycle",
				"status", "failed",
				"error_type", logging.ErrorType(err),
			)
			return
		}
		logger.Debug("otel tracer provider stopped", "component", "telemetry", "operation", "lifecycle")
	}()

	idempotencyStore, backend := buildIdempotencyStore()
	handler := &worker.DispatchEventHandler{
		Executor:         &loggingExecutor{},
		IdempotencyStore: idempotencyStore,
		IdempotencyTTL:   24 * time.Hour,
		WorkerName:       "kernelq-grpc-server",
		Logger:           logger,
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

	logger.Info(
		"worker ready",
		"component", "worker",
		"operation", "lifecycle",
		"status", "ready",
		"addr", cfg.Addr,
		"idempotency_backend", backend,
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(listener)
	}()

	health.MarkReady()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		logger.Info("worker shutting down", "component", "worker", "operation", "lifecycle", "status", "stopping")
		health.MarkNotReady()

		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
			logger.Debug("grpc graceful stop completed", "component", "worker", "operation", "lifecycle")
		case <-time.After(cfg.ShutdownTimeout):
			logger.Warn("grpc force stop after timeout", "component", "worker", "operation", "lifecycle")
			grpcServer.Stop()
		}
	case err := <-errCh:
		health.MarkNotReady()
		if err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
	}

	logger.Info("worker shutting down", "component", "worker", "operation", "lifecycle", "status", "stopped")
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
