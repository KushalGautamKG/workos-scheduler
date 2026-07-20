package telemetry_test

import (
	"testing"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"google.golang.org/grpc"
)

// Parent/child propagation lives in internal/grpc/trace_test.go (avoids import cycle:
// grpc → telemetry → grpc).

func TestGRPCServerOptionsNonEmpty(t *testing.T) {
	opts := telemetry.GRPCServerOptions()
	if len(opts) == 0 {
		t.Fatal("expected server options")
	}
	_ = grpc.NewServer(opts...)
}

func TestGRPCDialOptionsNonEmpty(t *testing.T) {
	opts := telemetry.GRPCDialOptions()
	if len(opts) == 0 {
		t.Fatal("expected dial options")
	}
}
