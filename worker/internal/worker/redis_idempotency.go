package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisSetNXClient is the Redis subset RedisIdempotencyStore needs.
// Tests inject a fake; production uses GoRedisSetNXClient wrapping *redis.Client.
type RedisSetNXClient interface {
	SetNX(
		ctx context.Context,
		key string,
		value string,
		expiration time.Duration,
	) (bool, error)
}

// RedisIdempotencyStore claims keys via Redis SET NX EX (through RedisSetNXClient).
// Logical keys (e.g. execution:job-abc:0) are prefixed with namespace
// (default convention: kernelq:idempotency).
type RedisIdempotencyStore struct {
	client    RedisSetNXClient
	namespace string
}

// NewRedisIdempotencyStore builds a Redis-backed IdempotencyStore.
// client and namespace must be non-nil / non-empty; callers choose the default
// namespace (typically "kernelq:idempotency").
func NewRedisIdempotencyStore(
	client RedisSetNXClient,
	namespace string,
) (*RedisIdempotencyStore, error) {
	if client == nil {
		return nil, fmt.Errorf("redis idempotency client must not be nil")
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, fmt.Errorf("redis idempotency namespace must be non-empty")
	}
	return &RedisIdempotencyStore{
		client:    client,
		namespace: namespace,
	}, nil
}

// TryClaim runs SET NX EX on namespace:key with value "1".
// Client errors propagate (fail closed for callers that treat err as fatal).
func (store *RedisIdempotencyStore) TryClaim(key string, ttl time.Duration) (bool, error) {
	if err := validateClaimInputs(key, ttl); err != nil {
		return false, err
	}

	redisKey := store.namespace + ":" + key
	claimed, err := store.client.SetNX(
		context.Background(),
		redisKey,
		"1",
		ttl,
	)
	if err != nil {
		return false, err
	}
	return claimed, nil
}

// Compile-time check that RedisIdempotencyStore implements IdempotencyStore.
var _ IdempotencyStore = (*RedisIdempotencyStore)(nil)

// GoRedisSetNXClient adapts *redis.Client to RedisSetNXClient.
type GoRedisSetNXClient struct {
	client *redis.Client
}

// NewGoRedisSetNXClient wraps a go-redis client. client must not be nil.
func NewGoRedisSetNXClient(client *redis.Client) (*GoRedisSetNXClient, error) {
	if client == nil {
		return nil, fmt.Errorf("go-redis client must not be nil")
	}
	return &GoRedisSetNXClient{client: client}, nil
}

// SetNX delegates to redis.Client.SetNX(...).Result().
func (adapter *GoRedisSetNXClient) SetNX(
	ctx context.Context,
	key string,
	value string,
	expiration time.Duration,
) (bool, error) {
	return adapter.client.SetNX(ctx, key, value, expiration).Result()
}

// Compile-time check that GoRedisSetNXClient implements RedisSetNXClient.
var _ RedisSetNXClient = (*GoRedisSetNXClient)(nil)
