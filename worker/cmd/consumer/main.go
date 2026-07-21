package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/redis/go-redis/v9"
)

const (
	bootstrapServers = "localhost:9092"
	consumerGroupID  = "kernelq-worker"
	dispatchTopic    = "kernelq.jobs.dispatch"
	pollTimeoutMs    = 1000

	defaultBackpressureHighRatio = 0.80
	defaultBackpressureLowRatio  = 0.50

	defaultIdempotencyTTLSeconds = 86400
	defaultRedisAddr             = "localhost:6379"
	defaultRedisNamespace        = "kernelq:idempotency"
)

// loggingExecutor is a no-op executor for local development.
// It prints the job id so we can see messages flowing through the stack.
type loggingExecutor struct {
	calls atomic.Int64
}

func (executor *loggingExecutor) Execute(task worker.Task) (worker.ExecutionResult, error) {
	executor.calls.Add(1)
	fmt.Printf("received task job_id=%s\n", task.JobID)
	return worker.SuccessResult(), nil
}

func (executor *loggingExecutor) Calls() int64 {
	return executor.calls.Load()
}

// positiveIntFromEnv reads an env var as a positive int. Missing, invalid, or <= 0 returns 0.
func positiveIntFromEnv(name string) int {
	raw := os.Getenv(name)
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0
	}
	return value
}

// boolFromEnv reads an env var as a bool. Missing or invalid returns defaultValue.
func boolFromEnv(name string, defaultValue bool) bool {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return defaultValue
	}
	return value
}

// floatFromEnv reads an env var as a float64. Missing or invalid returns defaultValue.
func floatFromEnv(name string, defaultValue float64) float64 {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return defaultValue
	}
	return value
}

func resolveIdempotencyTTL() time.Duration {
	seconds := positiveIntFromEnv("KERNELQ_WORKER_IDEMPOTENCY_TTL_SECONDS")
	if seconds == 0 {
		seconds = defaultIdempotencyTTLSeconds
	}
	return time.Duration(seconds) * time.Second
}

// buildIdempotencyStore configures the optional execution-dedupe store.
// Default backend is "disabled" (nil store) for backward compatibility.
func buildIdempotencyStore() (worker.IdempotencyStore, string, error) {
	backend := strings.ToLower(strings.TrimSpace(os.Getenv("KERNELQ_WORKER_IDEMPOTENCY_BACKEND")))
	if backend == "" {
		backend = "disabled"
	}

	switch backend {
	case "disabled":
		return nil, backend, nil
	case "memory":
		return worker.NewInMemoryIdempotencyStore(), backend, nil
	case "redis":
		addr := os.Getenv("KERNELQ_REDIS_ADDR")
		if strings.TrimSpace(addr) == "" {
			addr = defaultRedisAddr
		}
		namespace := os.Getenv("KERNELQ_REDIS_NAMESPACE")
		if strings.TrimSpace(namespace) == "" {
			namespace = defaultRedisNamespace
		}

		rdb := redis.NewClient(&redis.Options{Addr: addr})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := rdb.Ping(ctx).Err(); err != nil {
			_ = rdb.Close()
			return nil, backend, fmt.Errorf("redis ping %s: %w", addr, err)
		}

		adapter, err := worker.NewGoRedisSetNXClient(rdb)
		if err != nil {
			_ = rdb.Close()
			return nil, backend, err
		}
		store, err := worker.NewRedisIdempotencyStore(adapter, namespace)
		if err != nil {
			_ = rdb.Close()
			return nil, backend, err
		}
		return store, backend, nil
	default:
		return nil, backend, fmt.Errorf(
			"invalid KERNELQ_WORKER_IDEMPOTENCY_BACKEND %q (want disabled|memory|redis)",
			backend,
		)
	}
}

func main() {
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
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := tracerProvider.Shutdown(shutdownCtx); err != nil {
			fmt.Printf("event=otel_tracer_provider_shutdown error=%q\n", err.Error())
			return
		}
		fmt.Println("event=otel_tracer_provider_stopped")
	}()

	groupID := strings.TrimSpace(os.Getenv("KERNELQ_KAFKA_GROUP_ID"))
	if groupID == "" {
		groupID = consumerGroupID
	}
	autoOffsetReset := strings.TrimSpace(os.Getenv("KERNELQ_KAFKA_AUTO_OFFSET_RESET"))
	if autoOffsetReset == "" {
		autoOffsetReset = "earliest"
	}

	brokerConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupID,
		"auto.offset.reset": autoOffsetReset,
	})
	if err != nil {
		log.Fatalf("create kafka consumer: %v", err)
	}

	if err := brokerConsumer.Subscribe(dispatchTopic, nil); err != nil {
		_ = brokerConsumer.Close()
		log.Fatalf("subscribe to topic %q: %v", dispatchTopic, err)
	}

	dlqProducer, err := worker.NewKafkaDeadLetterProducer(bootstrapServers)
	if err != nil {
		_ = brokerConsumer.Close()
		log.Fatalf("create dlq producer: %v", err)
	}

	resultProducer, err := worker.NewKafkaResultProducer(bootstrapServers)
	if err != nil {
		_ = brokerConsumer.Close()
		log.Fatalf("create result producer: %v", err)
	}

	idempotencyStore, idempotencyBackend, err := buildIdempotencyStore()
	if err != nil {
		_ = brokerConsumer.Close()
		log.Fatalf("idempotency store: %v", err)
	}
	idempotencyTTL := resolveIdempotencyTTL()

	// Wire the worker stack: Kafka → decode → bounded worker pool → handler → executor → results + DLQ.
	// WorkerCount 0 => DefaultWorkerCount (4). QueueCapacity 0 => DefaultQueueCapacity (100).
	workerCountConfig := positiveIntFromEnv("KERNELQ_WORKER_COUNT")
	queueCapacity := positiveIntFromEnv("KERNELQ_WORKER_QUEUE_CAPACITY")
	backpressureEnabled := boolFromEnv("KERNELQ_WORKER_BACKPRESSURE_ENABLED", false)
	backpressureHighRatio := floatFromEnv(
		"KERNELQ_WORKER_BACKPRESSURE_HIGH_RATIO",
		defaultBackpressureHighRatio,
	)
	backpressureLowRatio := floatFromEnv(
		"KERNELQ_WORKER_BACKPRESSURE_LOW_RATIO",
		defaultBackpressureLowRatio,
	)

	executor := &loggingExecutor{}
	handler := &worker.DispatchEventHandler{
		Executor:         executor,
		ResultProducer:   resultProducer,
		WorkerName:       "kernelq-go-worker",
		IdempotencyStore: idempotencyStore,
		IdempotencyTTL:   idempotencyTTL,
	}

	kafkaConsumer := &worker.KafkaConsumer{
		Poller: brokerConsumer,
		Runner: worker.ConsumerRunner{
			Handler: handler,
		},
		WorkerCount:        workerCountConfig,
		QueueCapacity:      queueCapacity,
		DeadLetterProducer: dlqProducer,
	}

	if backpressureEnabled {
		policy := worker.NewBackpressurePolicy(backpressureHighRatio, backpressureLowRatio)
		kafkaConsumer.BackpressurePolicy = &policy
		kafkaConsumer.PauseResumeController = worker.NewInMemoryPauseResumeController()
	}

	workerCount := worker.DefaultWorkerCount
	if workerCountConfig > 0 {
		workerCount = workerCountConfig
	}
	effectiveQueueCapacity := worker.DefaultQueueCapacity
	if queueCapacity > 0 {
		effectiveQueueCapacity = queueCapacity
	}
	fmt.Printf(
		"KernelQ worker consumer started worker_count=%d queue_capacity=%d\n",
		workerCount,
		effectiveQueueCapacity,
	)
	fmt.Printf("backpressure_enabled=%t\n", backpressureEnabled)
	fmt.Printf("backpressure_high_ratio=%g\n", backpressureHighRatio)
	fmt.Printf("backpressure_low_ratio=%g\n", backpressureLowRatio)
	fmt.Printf("worker_idempotency_backend=%s\n", idempotencyBackend)
	fmt.Printf("worker_idempotency_ttl_seconds=%d\n", int(idempotencyTTL.Seconds()))
	fmt.Printf("kafka_group_id=%s\n", groupID)
	fmt.Printf("kafka_auto_offset_reset=%s\n", autoOffsetReset)

	// Stop cleanly on Ctrl+C or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := kafkaConsumer.Run(ctx, pollTimeoutMs); err != nil {
		log.Fatalf("consumer run failed: %v", err)
	}

	kafkaConsumer.Stats.DuplicateExecutions = int(handler.DuplicateExecutions())
	kafkaConsumer.Stats.IdempotencyErrors = int(handler.IdempotencyErrors())

	fmt.Println("KernelQ worker consumer stopped")
	fmt.Printf("executor_calls=%d\n", executor.Calls())
	for _, line := range worker.ConsumerShutdownStatsLines(kafkaConsumer.Stats) {
		fmt.Println(line)
	}
}
