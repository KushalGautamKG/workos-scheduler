package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	workergrpc "github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
)

// Thin CLI for gRPC Execute smokes — uses the shared Client (otelgrpc dial options).
func main() {
	addr := flag.String("addr", envOr("KERNELQ_GRPC_ADDR", "127.0.0.1:50051"), "gRPC server address")
	jobID := flag.String("job-id", "", "job id")
	attempt := flag.Int("attempt", 0, "attempt number")
	payload := flag.String("payload", "", "payload kind/value stored under key \"kind\"")
	timeout := flag.Duration("timeout", 5*time.Second, "RPC timeout")
	flag.Parse()

	if strings.TrimSpace(*jobID) == "" {
		fail("job-id is required")
	}

	otelCfg, err := telemetry.LoadConfig()
	if err != nil {
		fail(err.Error())
	}
	// CLI stays quiet unless smoke/dev explicitly enables OTel (avoids mixing
	// pretty-printed spans into status=SUCCESS stdout used by other smokes).
	if strings.TrimSpace(os.Getenv("KERNELQ_OTEL_ENABLED")) == "" {
		otelCfg.Enabled = false
		otelCfg.Exporter = telemetry.ExporterNone
	}
	provider, err := telemetry.NewTracerProvider(context.Background(), otelCfg)
	if err != nil {
		fail(err.Error())
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = provider.Shutdown(shutdownCtx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout+2*time.Second)
	defer cancel()

	client, err := workergrpc.NewClient(ctx, *addr, *timeout)
	if err != nil {
		fail(err.Error())
	}
	defer client.Close()

	var payloadMap map[string]string
	if strings.TrimSpace(*payload) != "" {
		payloadMap = map[string]string{"kind": *payload}
	}

	resp, err := client.Execute(ctx, *jobID, int32(*attempt), payloadMap)
	if err != nil {
		fail(err.Error())
	}

	fmt.Printf("status=%s\n", resp.GetStatus())
	fmt.Printf("duplicate_skipped=%t\n", resp.GetDuplicateSkipped())
	if resp.GetErrorMessage() != "" {
		fmt.Printf("error_message=%s\n", resp.GetErrorMessage())
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func fail(message string) {
	fmt.Fprintf(os.Stderr, "FAIL: %s\n", message)
	os.Exit(1)
}
