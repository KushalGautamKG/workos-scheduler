package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	bootstrapServers = "localhost:9092"
	consumerGroupID  = "kernelq-worker"
	dispatchTopic    = "kernelq.jobs.dispatch"
	pollTimeoutMs    = 1000

	defaultBackpressureHighRatio = 0.80
	defaultBackpressureLowRatio  = 0.50
)

// loggingExecutor is a no-op executor for local development.
// It prints the job id so we can see messages flowing through the stack.
type loggingExecutor struct{}

func (loggingExecutor) Execute(task worker.Task) (worker.ExecutionResult, error) {
	fmt.Printf("received task job_id=%s\n", task.JobID)
	return worker.SuccessResult(), nil
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

func main() {
	brokerConsumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          consumerGroupID,
		"auto.offset.reset": "earliest",
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

	kafkaConsumer := &worker.KafkaConsumer{
		Poller: brokerConsumer,
		Runner: worker.ConsumerRunner{
			Handler: worker.DispatchEventHandler{
				Executor:       loggingExecutor{},
				ResultProducer: resultProducer,
				WorkerName:     "kernelq-go-worker",
			},
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

	// Stop cleanly on Ctrl+C or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := kafkaConsumer.Run(ctx, pollTimeoutMs); err != nil {
		log.Fatalf("consumer run failed: %v", err)
	}

	fmt.Println("KernelQ worker consumer stopped")
	for _, line := range worker.ConsumerShutdownStatsLines(kafkaConsumer.Stats) {
		fmt.Println(line)
	}
}
