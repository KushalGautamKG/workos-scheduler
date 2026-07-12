package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/KushalGautamKG/workos-scheduler/worker/internal/worker"
	"github.com/redis/go-redis/v9"
)

// Live Redis smoke for RedisIdempotencyStore (Day 111).
// Run via worker/scripts/smoke_redis_idempotency.sh — not part of unit tests.
func main() {
	addr := envOr("KERNELQ_REDIS_ADDR", "localhost:6379")
	namespace := envOr("KERNELQ_REDIS_NAMESPACE", "kernelq:idempotency")
	key := fmt.Sprintf("execution:smoke-go-%d:0", time.Now().UnixNano())

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fail(fmt.Sprintf("redis ping failed: %v", err))
	}

	adapter, err := worker.NewGoRedisSetNXClient(rdb)
	if err != nil {
		fail(err.Error())
	}
	store, err := worker.NewRedisIdempotencyStore(adapter, namespace)
	if err != nil {
		fail(err.Error())
	}

	first, err := store.TryClaim(key, 24*time.Hour)
	if err != nil {
		fail(fmt.Sprintf("first claim: %v", err))
	}
	second, err := store.TryClaim(key, 24*time.Hour)
	if err != nil {
		fail(fmt.Sprintf("second claim: %v", err))
	}

	fmt.Printf("first_claim=%t\n", first)
	fmt.Printf("second_claim=%t\n", second)

	if !first || second {
		fail(fmt.Sprintf("expected first=true second=false, got first=%t second=%t", first, second))
	}

	fmt.Println("PASS: go redis idempotency smoke test succeeded")
	fmt.Println("event=smoke_go_redis_idempotency success=true")
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func fail(message string) {
	fmt.Fprintf(os.Stderr, "FAIL: %s\n", message)
	fmt.Println("event=smoke_go_redis_idempotency success=false")
	os.Exit(1)
}
