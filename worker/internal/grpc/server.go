// Package grpc implements the local WorkerExecutionService skeleton (Day 116).
//
// No network listener yet — unit tests call Server.Execute directly. Kafka
// remains the async dispatch path; this RPC boundary prepares for OTel later.
package grpc

import (
	"context"
	"fmt"
	"strings"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc/pb"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Proto response status strings (kept small on purpose).
const (
	StatusSuccess          = "SUCCESS"
	StatusFailed           = "FAILED"
	StatusDuplicateSkipped = "DUPLICATE_SKIPPED"
)

// ExecutionHandler is the internal worker boundary used by the gRPC server.
// *worker.DispatchEventHandler satisfies this interface.
type ExecutionHandler interface {
	Handle(event worker.DispatchEvent) (worker.ExecutionResult, error)
}

// Server implements pb.WorkerExecutionServiceServer without starting a listener.
type Server struct {
	pb.UnimplementedWorkerExecutionServiceServer

	Handler ExecutionHandler
}

// Execute validates the request, maps it onto a DispatchEvent, and delegates
// to the wired ExecutionHandler. When Handler is nil, returns Unimplemented.
func (s *Server) Execute(
	ctx context.Context,
	req *pb.ExecuteRequest,
) (*pb.ExecuteResponse, error) {
	_ = ctx

	if s == nil || s.Handler == nil {
		return nil, status.Error(codes.Unimplemented, "execution handler not wired")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if err := validateExecuteRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	event := worker.DispatchEvent{
		EventType: "job.dispatch",
		JobID:     strings.TrimSpace(req.GetJobId()),
		TenantID:  "grpc-internal",
		Priority:  0,
		State:     "dispatched",
		Payload:   req.GetPayload(),
		Attempt:   int(req.GetAttempt()),
	}

	result, err := s.Handler.Handle(event)
	if err != nil {
		return &pb.ExecuteResponse{
			Status:         StatusFailed,
			ErrorMessage:   err.Error(),
			DuplicateSkipped: false,
		}, nil
	}

	return mapExecutionResult(result), nil
}

func validateExecuteRequest(req *pb.ExecuteRequest) error {
	if strings.TrimSpace(req.GetJobId()) == "" {
		return fmt.Errorf("job_id must not be blank")
	}
	if req.GetAttempt() < 0 {
		return fmt.Errorf("attempt must be >= 0, got %d", req.GetAttempt())
	}
	return nil
}

func mapExecutionResult(result worker.ExecutionResult) *pb.ExecuteResponse {
	switch result.Status {
	case worker.ExecutionSucceeded:
		return &pb.ExecuteResponse{
			Status:           StatusSuccess,
			DuplicateSkipped: false,
			ErrorMessage:     result.Message,
		}
	case worker.ExecutionDuplicateSkipped:
		return &pb.ExecuteResponse{
			Status:           StatusDuplicateSkipped,
			DuplicateSkipped: true,
			ErrorMessage:     result.Message,
		}
	case worker.ExecutionRetryableFailure, worker.ExecutionTerminalFailure:
		return &pb.ExecuteResponse{
			Status:           StatusFailed,
			DuplicateSkipped: false,
			ErrorMessage:     result.Message,
		}
	default:
		return &pb.ExecuteResponse{
			Status:           StatusFailed,
			DuplicateSkipped: false,
			ErrorMessage:     fmt.Sprintf("unknown execution status %q", result.Status),
		}
	}
}
