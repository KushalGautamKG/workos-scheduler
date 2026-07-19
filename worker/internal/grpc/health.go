package grpc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// OverallHealthService is the empty service name used for process readiness.
const OverallHealthService = ""

// Health wraps the official grpc.health.v1 server for readiness lifecycle.
// Starts NOT_SERVING until MarkReady after initialization.
type Health struct {
	server *health.Server
}

// NewHealth creates a health server marked NOT_SERVING.
func NewHealth() *Health {
	h := &Health{server: health.NewServer()}
	h.MarkNotReady()
	return h
}

// Register attaches grpc.health.v1 onto the gRPC server.
func (h *Health) Register(registrar grpc.ServiceRegistrar) {
	if h == nil || h.server == nil {
		return
	}
	grpc_health_v1.RegisterHealthServer(registrar, h.server)
}

// MarkReady sets overall status to SERVING (ready for traffic).
func (h *Health) MarkReady() {
	if h == nil || h.server == nil {
		return
	}
	h.server.SetServingStatus(OverallHealthService, grpc_health_v1.HealthCheckResponse_SERVING)
}

// MarkNotReady sets overall status to NOT_SERVING (startup or shutdown).
func (h *Health) MarkNotReady() {
	if h == nil || h.server == nil {
		return
	}
	h.server.SetServingStatus(OverallHealthService, grpc_health_v1.HealthCheckResponse_NOT_SERVING)
}

// Status returns the overall readiness status string: SERVING or NOT_SERVING.
func (h *Health) Status(ctx context.Context) (string, error) {
	if h == nil || h.server == nil {
		return "", fmt.Errorf("health server is not initialized")
	}
	resp, err := h.server.Check(ctx, &grpc_health_v1.HealthCheckRequest{
		Service: OverallHealthService,
	})
	if err != nil {
		return "", err
	}
	return resp.GetStatus().String(), nil
}
