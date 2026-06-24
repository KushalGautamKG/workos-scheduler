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

	// Stop cleanly on Ctrl+C or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := kafkaConsumer.Run(ctx, pollTimeoutMs); err != nil {
		log.Fatalf("consumer run failed: %v", err)
	}

	fmt.Println("KernelQ worker consumer stopped")
	fmt.Printf("messages_seen=%d\n", kafkaConsumer.Stats.MessagesSeen)
	fmt.Printf("messages_processed=%d\n", kafkaConsumer.Stats.MessagesProcessed)
	fmt.Printf("message_errors=%d\n", kafkaConsumer.Stats.MessageErrors)
	fmt.Printf("kafka_errors=%d\n", kafkaConsumer.Stats.KafkaErrors)
	fmt.Printf("work_queue_capacity=%d\n", kafkaConsumer.Stats.WorkQueueCapacity)
	fmt.Printf("work_items_enqueued=%d\n", kafkaConsumer.Stats.WorkItemsEnqueued)
	fmt.Printf("work_queue_full_errors=%d\n", kafkaConsumer.Stats.WorkQueueFullErrors)
}
