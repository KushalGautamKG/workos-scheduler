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

	workergrpc "github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc/pb"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"google.golang.org/grpc"
)

const defaultGRPCAddr = "127.0.0.1:50051"

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
	addr := strings.TrimSpace(os.Getenv("KERNELQ_GRPC_ADDR"))
	if addr == "" {
		addr = defaultGRPCAddr
	}

	idempotencyStore, backend := buildIdempotencyStore()
	handler := &worker.DispatchEventHandler{
		Executor:         &loggingExecutor{},
		IdempotencyStore: idempotencyStore,
		IdempotencyTTL:   24 * time.Hour,
		WorkerName:       "kernelq-grpc-server",
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterWorkerExecutionServiceServer(grpcServer, &workergrpc.Server{
		Handler: handler,
	})

	fmt.Printf("event=grpc_server_start addr=%s idempotency_backend=%s\n", addr, backend)

	errCh := make(chan error, 1)
	go func() {
		errCh <- grpcServer.Serve(listener)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		fmt.Println("event=grpc_server_shutdown reason=signal")
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			fmt.Println("event=grpc_server_shutdown reason=force_stop")
			grpcServer.Stop()
		}
	case err := <-errCh:
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
		// Unknown value: fail closed to memory so local smokes stay predictable.
		fmt.Printf(
			"event=grpc_server_idempotency_fallback requested=%s using=memory\n",
			backend,
		)
		return worker.NewInMemoryIdempotencyStore(), "memory"
	}
}
