package worker

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// fakeKafkaProducerClient records Produce and Flush calls without a broker.
type fakeKafkaProducerClient struct {
	messages       []*kafka.Message
	produceErr     error
	flushRemaining int
	flushTimeoutMs int
	flushCalled    bool
}

func (client *fakeKafkaProducerClient) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	if client.produceErr != nil {
		return client.produceErr
	}
	client.messages = append(client.messages, msg)
	if deliveryChan != nil {
		deliveryChan <- msg
	}
	return nil
}

func (client *fakeKafkaProducerClient) Flush(timeoutMs int) int {
	client.flushCalled = true
	client.flushTimeoutMs = timeoutMs
	return client.flushRemaining
}

func newTestKafkaDeadLetterProducer(client *fakeKafkaProducerClient) KafkaDeadLetterProducer {
	return KafkaDeadLetterProducer{
		Producer: client,
		Topic:    DLQTopic,
	}
}

func TestPublishDeadLetterSendsMessageToDLQTopic(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaDeadLetterProducer(fakeClient)

	err := producer.PublishDeadLetter(validDeadLetterEvent())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if len(fakeClient.messages) != 1 {
		t.Fatalf("expected 1 produced message, got %d", len(fakeClient.messages))
	}
	if fakeClient.messages[0].TopicPartition.Topic == nil {
		t.Fatal("expected topic on produced message")
	}
	if *fakeClient.messages[0].TopicPartition.Topic != DLQTopic {
		t.Fatalf("expected topic %q, got %q", DLQTopic, *fakeClient.messages[0].TopicPartition.Topic)
	}
}

func TestPublishDeadLetterUsesOriginalKeyAsKafkaKey(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaDeadLetterProducer(fakeClient)
	event := validDeadLetterEvent()

	err := producer.PublishDeadLetter(event)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	if string(fakeClient.messages[0].Key) != event.OriginalKey {
		t.Fatalf("expected key %q, got %q", event.OriginalKey, string(fakeClient.messages[0].Key))
	}
}

func TestPublishDeadLetterValueContainsReasonAndOriginalValueJSON(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaDeadLetterProducer(fakeClient)
	event := validDeadLetterEvent()

	err := producer.PublishDeadLetter(event)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	var decoded map[string]string
	if err := json.Unmarshal(fakeClient.messages[0].Value, &decoded); err != nil {
		t.Fatalf("expected valid JSON value, got error: %v", err)
	}
	if decoded["reason"] != event.Reason {
		t.Fatalf("expected reason %q, got %q", event.Reason, decoded["reason"])
	}
	if decoded["original_value"] != event.OriginalValue {
		t.Fatalf("expected original_value %q, got %q", event.OriginalValue, decoded["original_value"])
	}
}

func TestPublishDeadLetterCallsFlush(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaDeadLetterProducer(fakeClient)

	err := producer.PublishDeadLetter(validDeadLetterEvent())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if !fakeClient.flushCalled {
		t.Fatal("expected Flush to be called")
	}
	if fakeClient.flushTimeoutMs != dlqFlushTimeoutMs {
		t.Fatalf("expected flush timeout %d, got %d", dlqFlushTimeoutMs, fakeClient.flushTimeoutMs)
	}
}

func TestPublishDeadLetterReturnsErrorForInvalidEventWithoutProducing(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaDeadLetterProducer(fakeClient)

	event := validDeadLetterEvent()
	event.Reason = "   "

	err := producer.PublishDeadLetter(event)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "reason") {
		t.Fatalf("expected reason error, got: %v", err)
	}
	if len(fakeClient.messages) != 0 {
		t.Fatalf("expected no produced messages, got %d", len(fakeClient.messages))
	}
	if fakeClient.flushCalled {
		t.Fatal("Flush should not be called for invalid event")
	}
}

func TestPublishDeadLetterReturnsProduceError(t *testing.T) {
	expectedErr := fmt.Errorf("simulated produce failure")
	fakeClient := &fakeKafkaProducerClient{produceErr: expectedErr}
	producer := newTestKafkaDeadLetterProducer(fakeClient)

	err := producer.PublishDeadLetter(validDeadLetterEvent())
	if err == nil {
		t.Fatal("expected produce error, got nil")
	}
	if !strings.Contains(err.Error(), "produce dead-letter event") {
		t.Fatalf("expected produce error wrapper, got: %v", err)
	}
}

func TestPublishDeadLetterRespectsCustomTopic(t *testing.T) {
	const customTopic = "custom.test.dlq"
	fakeClient := &fakeKafkaProducerClient{}
	producer := KafkaDeadLetterProducer{
		Producer: fakeClient,
		Topic:    customTopic,
	}

	err := producer.PublishDeadLetter(validDeadLetterEvent())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if fakeClient.messages[0].TopicPartition.Topic == nil {
		t.Fatal("expected topic on produced message")
	}
	if *fakeClient.messages[0].TopicPartition.Topic != customTopic {
		t.Fatalf("expected topic %q, got %q", customTopic, *fakeClient.messages[0].TopicPartition.Topic)
	}
}
