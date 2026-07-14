package worker

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func newTestKafkaResultProducer(client *fakeKafkaProducerClient) KafkaResultProducer {
	return KafkaResultProducer{
		Producer: client,
		Topic:    ResultTopic,
	}
}

func TestPublishResultSendsMessageToResultsTopic(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaResultProducer(fakeClient)

	err := producer.PublishResult(validWorkerResultEvent())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if len(fakeClient.messages) != 1 {
		t.Fatalf("expected 1 produced message, got %d", len(fakeClient.messages))
	}
	if fakeClient.messages[0].TopicPartition.Topic == nil {
		t.Fatal("expected topic on produced message")
	}
	if *fakeClient.messages[0].TopicPartition.Topic != ResultTopic {
		t.Fatalf("expected topic %q, got %q", ResultTopic, *fakeClient.messages[0].TopicPartition.Topic)
	}
}

func TestPublishResultUsesJobIDAsKafkaKey(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaResultProducer(fakeClient)
	event := validWorkerResultEvent()

	err := producer.PublishResult(event)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	if string(fakeClient.messages[0].Key) != event.JobID {
		t.Fatalf("expected key %q, got %q", event.JobID, string(fakeClient.messages[0].Key))
	}
}

func TestPublishResultValueContainsExpectedJSONFields(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaResultProducer(fakeClient)
	event := validWorkerResultEvent()

	err := producer.PublishResult(event)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	var decoded map[string]string
	if err := json.Unmarshal(fakeClient.messages[0].Value, &decoded); err != nil {
		t.Fatalf("expected valid JSON value, got error: %v", err)
	}
	if decoded["event_type"] != "job.result" {
		t.Fatalf("expected event_type job.result, got %q", decoded["event_type"])
	}
	if decoded["job_id"] != event.JobID {
		t.Fatalf("expected job_id %q, got %q", event.JobID, decoded["job_id"])
	}
	if decoded["status"] != string(ExecutionSucceeded) {
		t.Fatalf("expected status %q, got %q", ExecutionSucceeded, decoded["status"])
	}
	if decoded["worker"] != testWorkerIdentity {
		t.Fatalf("expected worker %q, got %q", testWorkerIdentity, decoded["worker"])
	}
}

func TestPublishResultWaitsForDelivery(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaResultProducer(fakeClient)

	err := producer.PublishResult(validWorkerResultEvent())
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}
	if len(fakeClient.messages) != 1 {
		t.Fatalf("expected 1 produced message, got %d", len(fakeClient.messages))
	}
	if fakeClient.flushCalled {
		t.Fatal("delivery-channel publish should not call Flush")
	}
}

func TestPublishResultReturnsErrorForInvalidEventWithoutProducing(t *testing.T) {
	fakeClient := &fakeKafkaProducerClient{}
	producer := newTestKafkaResultProducer(fakeClient)

	event := validWorkerResultEvent()
	event.JobID = "   "

	err := producer.PublishResult(event)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "job_id") {
		t.Fatalf("expected job_id error, got: %v", err)
	}
	if len(fakeClient.messages) != 0 {
		t.Fatalf("expected no produced messages, got %d", len(fakeClient.messages))
	}
	if fakeClient.flushCalled {
		t.Fatal("Flush should not be called for invalid event")
	}
}

func TestPublishResultReturnsProduceError(t *testing.T) {
	expectedErr := fmt.Errorf("simulated produce failure")
	fakeClient := &fakeKafkaProducerClient{produceErr: expectedErr}
	producer := newTestKafkaResultProducer(fakeClient)

	err := producer.PublishResult(validWorkerResultEvent())
	if err == nil {
		t.Fatal("expected produce error, got nil")
	}
	if !strings.Contains(err.Error(), "produce result event") {
		t.Fatalf("expected produce error wrapper, got: %v", err)
	}
}

func TestPublishResultRespectsCustomTopic(t *testing.T) {
	const customTopic = "custom.test.results"
	fakeClient := &fakeKafkaProducerClient{}
	producer := KafkaResultProducer{
		Producer: fakeClient,
		Topic:    customTopic,
	}

	err := producer.PublishResult(validWorkerResultEvent())
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
