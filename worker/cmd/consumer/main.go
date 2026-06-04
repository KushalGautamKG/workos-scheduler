package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
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

func (loggingExecutor) Execute(task worker.Task) error {
	fmt.Printf("received task job_id=%s\n", task.JobID)
	return nil
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

	// Wire the worker stack: Kafka → parse/validate → handler → executor.
	kafkaConsumer := worker.KafkaConsumer{
		Poller: brokerConsumer,
		Runner: worker.ConsumerRunner{
			Handler: worker.DispatchEventHandler{
				Executor: loggingExecutor{},
			},
		},
	}

	fmt.Println("KernelQ worker consumer started")

	// Stop cleanly on Ctrl+C or SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := kafkaConsumer.Run(ctx, pollTimeoutMs); err != nil {
		log.Fatalf("consumer run failed: %v", err)
	}

	fmt.Println("KernelQ worker consumer stopped")
}
