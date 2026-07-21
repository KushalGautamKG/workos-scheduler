package worker_test

import (
	"context"
	"errors"
	"testing"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func setupWorkerTrace(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return exporter
}

type successExec struct{}

func (successExec) Execute(task worker.Task) (worker.ExecutionResult, error) {
	_ = task
	return worker.SuccessResult(), nil
}

type failExec struct{}

func (failExec) Execute(task worker.Task) (worker.ExecutionResult, error) {
	_ = task
	return worker.ExecutionResult{}, errors.New("forced execute failure")
}

func sampleDispatch(jobID string) worker.DispatchEvent {
	return worker.DispatchEvent{
		EventType: "job.dispatch",
		JobID:     jobID,
		TenantID:  "tenant-a",
		Priority:  1,
		State:     "dispatched",
		Payload:   map[string]string{"kind": "trace"},
		Attempt:   0,
	}
}

func TestKafkaTraceHierarchyPublishProcessExecuteResult(t *testing.T) {
	exporter := setupWorkerTrace(t)
	results := &worker.RecordingResultProducer{}
	handler := &worker.DispatchEventHandler{
		Executor:       successExec{},
		ResultProducer: results,
		WorkerName:     "trace-test",
	}

	rootCtx, root := otel.Tracer("test").Start(context.Background(), "test.root")
	headers := []kafka.Header{{Key: "x-custom", Value: []byte("1")}}
	pubCtx, pubSpan := telemetry.StartKafkaPublishSpan(rootCtx, worker.DispatchTopic, "publish")
	telemetry.InjectKafkaContext(pubCtx, &headers)
	pubSpan.End()
	root.End()

	msg := worker.Message{
		Key:       "job-trace",
		Value:     mustJSON(t, sampleDispatch("job-trace")),
		Headers:   headers,
		Topic:     worker.DispatchTopic,
		Partition: 0,
		Offset:    7,
	}
	runner := worker.ConsumerRunner{Handler: handler}
	if err := runner.ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}

	spans := exporter.GetSpans()
	byID := map[trace.SpanID]tracetest.SpanStub{}
	for _, s := range spans {
		byID[s.SpanContext.SpanID()] = s
	}
	var pub, proc, exec, resultPub *tracetest.SpanStub
	for i := range spans {
		s := &spans[i]
		switch s.Name {
		case telemetry.SpanKafkaPublish:
			parentName := ""
			if parent, ok := byID[s.Parent.SpanID()]; ok {
				parentName = parent.Name
			}
			if parentName == telemetry.SpanWorkerExecute {
				resultPub = s
			} else if pub == nil {
				pub = s
			}
		case telemetry.SpanKafkaProcess:
			proc = s
		case telemetry.SpanWorkerExecute:
			exec = s
		}
	}
	if pub == nil || proc == nil || exec == nil || resultPub == nil {
		t.Fatalf("missing spans among %v", spanNames(spans))
	}
	if pub.SpanKind != trace.SpanKindProducer || resultPub.SpanKind != trace.SpanKindProducer {
		t.Fatal("publish spans must be producer kind")
	}
	if proc.SpanKind != trace.SpanKindConsumer {
		t.Fatal("process span must be consumer kind")
	}
	tid := pub.SpanContext.TraceID()
	for _, s := range []*tracetest.SpanStub{proc, exec, resultPub} {
		if s.SpanContext.TraceID() != tid {
			t.Fatalf("span %q not on publish trace", s.Name)
		}
		if s.EndTime.IsZero() {
			t.Fatalf("span %q missing end", s.Name)
		}
	}
	if !proc.Parent.IsRemote() {
		t.Fatal("process span should have remote parent from headers")
	}
	if len(results.Published) != 1 {
		t.Fatalf("expected result publish, got %d", len(results.Published))
	}
	if len(results.Headers) != 1 || !hasTraceparent(results.Headers[0]) {
		t.Fatal("result publish should inject traceparent")
	}
	preserved := false
	for _, h := range headers {
		if h.Key == "x-custom" {
			preserved = true
		}
	}
	if !preserved {
		t.Fatal("custom header lost")
	}
}

func TestKafkaTraceMissingHeadersStillExecutes(t *testing.T) {
	exporter := setupWorkerTrace(t)
	handler := &worker.DispatchEventHandler{Executor: successExec{}}
	msg := worker.Message{
		Key:   "job-nohdr",
		Value: mustJSON(t, sampleDispatch("job-nohdr")),
		Topic: worker.DispatchTopic,
	}
	if err := (worker.ConsumerRunner{Handler: handler}).ProcessMessage(context.Background(), msg); err != nil {
		t.Fatalf("ProcessMessage: %v", err)
	}
	var sawProc, sawExec bool
	for _, s := range exporter.GetSpans() {
		if s.Name == telemetry.SpanKafkaProcess {
			sawProc = true
		}
		if s.Name == telemetry.SpanWorkerExecute {
			sawExec = true
		}
	}
	if !sawProc || !sawExec {
		t.Fatalf("expected process+execute without headers; got %v", spanNames(exporter.GetSpans()))
	}
}

func TestKafkaTraceHandlerFailureRecordsProcessError(t *testing.T) {
	exporter := setupWorkerTrace(t)
	handler := &worker.DispatchEventHandler{Executor: failExec{}}
	msg := worker.Message{
		Key:   "job-fail",
		Value: mustJSON(t, sampleDispatch("job-fail")),
		Topic: worker.DispatchTopic,
	}
	err := (worker.ConsumerRunner{Handler: handler}).ProcessMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected handler error")
	}
	var sawErr bool
	for _, s := range exporter.GetSpans() {
		if s.Name == telemetry.SpanKafkaProcess && len(s.Events) > 0 {
			sawErr = true
		}
	}
	if !sawErr {
		t.Fatal("process span should record handler error")
	}
}

func TestKafkaPublishFailureRecordsError(t *testing.T) {
	exporter := setupWorkerTrace(t)
	producer := &worker.KafkaDispatchProducer{
		Producer: &failingKafkaClient{},
		Topic:    worker.DispatchTopic,
	}
	err := producer.PublishDispatch(context.Background(), sampleDispatch("job-pub-fail"))
	if err == nil {
		t.Fatal("expected produce error")
	}
	found := false
	for _, s := range exporter.GetSpans() {
		if s.Name == telemetry.SpanKafkaPublish && len(s.Events) > 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("publish span should record error")
	}
}

type failingKafkaClient struct{}

func (failingKafkaClient) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	_ = msg
	_ = deliveryChan
	return errors.New("broker unavailable")
}

func (failingKafkaClient) Flush(timeoutMs int) int {
	_ = timeoutMs
	return 0
}

func mustJSON(t *testing.T, event worker.DispatchEvent) []byte {
	t.Helper()
	b, err := event.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func spanNames(spans tracetest.SpanStubs) []string {
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}

func hasTraceparent(headers []kafka.Header) bool {
	for _, h := range headers {
		if h.Key == "traceparent" || h.Key == "Traceparent" {
			return len(h.Value) > 0
		}
	}
	return false
}
