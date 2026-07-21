package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"go.opentelemetry.io/otel"
)

// Publishes one instrumented dispatch event (Day 122 smoke helper).
func main() {
	bootstrap := flag.String("bootstrap", envOr("KERNELQ_KAFKA_BOOTSTRAP", "localhost:9092"), "Kafka bootstrap servers")
	jobID := flag.String("job-id", "", "job id")
	tenantID := flag.String("tenant-id", "tenant-smoke", "tenant id")
	attempt := flag.Int("attempt", 0, "attempt")
	payload := flag.String("payload", "smoke", "payload kind")
	flag.Parse()

	if strings.TrimSpace(*jobID) == "" {
		fail("job-id is required")
	}

	otelCfg, err := telemetry.LoadConfig()
	if err != nil {
		fail(err.Error())
	}
	if strings.TrimSpace(os.Getenv("KERNELQ_OTEL_ENABLED")) == "" {
		otelCfg.Enabled = false
		otelCfg.Exporter = telemetry.ExporterNone
	}
	provider, err := telemetry.NewTracerProvider(context.Background(), otelCfg)
	if err != nil {
		fail(err.Error())
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = provider.Shutdown(ctx)
	}()

	producer, err := worker.NewKafkaDispatchProducer(*bootstrap)
	if err != nil {
		fail(err.Error())
	}
	defer producer.Close()

	rootCtx, root := otel.Tracer("kernelq.smoke").Start(context.Background(), "smoke.dispatch.root")
	defer root.End()

	event := worker.DispatchEvent{
		EventType: "job.dispatch",
		JobID:     *jobID,
		TenantID:  *tenantID,
		Priority:  1,
		State:     "dispatched",
		Payload:   map[string]string{"kind": *payload},
		Attempt:   *attempt,
	}
	if err := producer.PublishDispatch(rootCtx, event); err != nil {
		fail(err.Error())
	}
	fmt.Printf("event=dispatch_published job_id=%s topic=%s\n", event.JobID, worker.DispatchTopic)
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func fail(msg string) {
	fmt.Fprintf(os.Stderr, "FAIL: %s\n", msg)
	os.Exit(1)
}
