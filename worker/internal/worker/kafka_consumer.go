// Package worker holds types and logic for the KernelQ worker plane.
//
// This file adapts confluent-kafka-go records into our in-memory Message type.
// A future poll loop will read from the broker and call ProcessKafkaMessage
// for each record—no polling here yet.
package worker

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// KafkaConsumer wraps a broker client and our message-processing runner.
//
// The Kafka SDK delivers *kafka.Message values; ConsumerRunner knows how to
// parse, validate, and hand off to a DispatchHandler. This struct connects
// the two layers without mixing broker code into business logic.
type KafkaConsumer struct {
	Consumer *kafka.Consumer
	Runner   ConsumerRunner
}

// ProcessKafkaMessage handles one record from the Kafka client.
//
// Call this from a poll loop later. Today tests can pass a *kafka.Message
// directly without starting a real consumer loop.
func (c KafkaConsumer) ProcessKafkaMessage(msg *kafka.Message) error {
	if msg == nil {
		return fmt.Errorf("kafka message is nil")
	}

	// Map broker record fields onto our simple Message type.
	message := Message{
		Key:   string(msg.Key),
		Value: msg.Value,
	}

	if err := c.Runner.ProcessMessage(message); err != nil {
		return err
	}

	return nil
}
