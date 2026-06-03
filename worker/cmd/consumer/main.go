package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

const (
	bootstrapServers = "localhost:9092"
	consumerGroupID  = "kernelq-worker"
	dispatchTopic    = "kernelq.jobs.dispatch"
)

func main() {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          consumerGroupID,
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("create kafka consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Subscribe(dispatchTopic, nil); err != nil {
		log.Fatalf("subscribe to topic %q: %v", dispatchTopic, err)
	}

	fmt.Println("KernelQ worker consumer started")
}
