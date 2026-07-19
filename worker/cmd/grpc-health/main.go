package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
)

// Thin CLI to query grpc.health.v1 for Day 118 smoke.
func main() {
	cfg, err := config.LoadGRPCConfig()
	if err != nil {
		fail(err.Error())
	}

	addr := flag.String("addr", cfg.Addr, "gRPC server address")
	service := flag.String("service", "", "health service name (empty = overall)")
	timeout := flag.Duration("timeout", cfg.RequestTimeout, "RPC timeout")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		fail(fmt.Sprintf("dial %s: %v", *addr, err))
	}
	defer conn.Close()

	client := grpc_health_v1.NewHealthClient(conn)
	resp, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{Service: *service})
	if err != nil {
		fail(err.Error())
	}

	fmt.Printf("status=%s\n", resp.GetStatus().String())
}

func fail(message string) {
	fmt.Fprintf(os.Stderr, "FAIL: %s\n", message)
	os.Exit(1)
}
