package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/grpc/pb"
	"github.com/KushalGautamKG/workos-scheduler/worker/internal/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DefaultClientTimeout bounds a single Execute RPC when the caller passes
// context.Background() without a deadline.
const DefaultClientTimeout = 5 * time.Second

// Client is a thin WorkerExecutionService client over a gRPC connection.
// No retries yet — callers own backoff if needed.
type Client struct {
	conn    *grpc.ClientConn
	stub    pb.WorkerExecutionServiceClient
	timeout time.Duration
}

// NewClient dials endpoint with insecure credentials (local/dev only).
// timeout <= 0 uses DefaultClientTimeout.
// Applies official otelgrpc client StatsHandler for trace propagation.
func NewClient(ctx context.Context, endpoint string, timeout time.Duration) (*Client, error) {
	if endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if timeout <= 0 {
		timeout = DefaultClientTimeout
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	}
	opts = append(opts, telemetry.GRPCDialOptions()...)

	conn, err := grpc.DialContext(ctx, endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", endpoint, err)
	}

	return &Client{
		conn:    conn,
		stub:    pb.NewWorkerExecutionServiceClient(conn),
		timeout: timeout,
	}, nil
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// Execute calls WorkerExecutionService.Execute with a context timeout derived
// from the caller context (preserves parent trace + deadlines).
func (c *Client) Execute(
	ctx context.Context,
	jobID string,
	attempt int32,
	payload map[string]string,
) (*pb.ExecuteResponse, error) {
	if c == nil || c.stub == nil {
		return nil, fmt.Errorf("client is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Derive from caller ctx — never Background — so trace/deadline propagate.
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.stub.Execute(callCtx, &pb.ExecuteRequest{
		JobId:   jobID,
		Attempt: attempt,
		Payload: payload,
	})
}
