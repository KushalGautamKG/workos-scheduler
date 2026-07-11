package worker

import (
	"sync"
	"time"
)

// InMemoryIdempotencyStore is a process-local IdempotencyStore for unit tests
// and early development. It is not shared across processes — production will
// use a Redis-backed store that honors TTL via SET NX EX.
//
// TTL is accepted (and validated) for interface compatibility but expiration
// is intentionally ignored: once claimed, a key stays claimed until the
// process exits. That keeps tests deterministic without a clock.
type InMemoryIdempotencyStore struct {
	mu      sync.Mutex
	claimed map[string]struct{}
}

// NewInMemoryIdempotencyStore returns an empty in-memory store.
func NewInMemoryIdempotencyStore() *InMemoryIdempotencyStore {
	return &InMemoryIdempotencyStore{
		claimed: make(map[string]struct{}),
	}
}

// TryClaim records key on first success and returns false for duplicates.
// ttl must be > 0 but is not used for expiry in this implementation.
func (store *InMemoryIdempotencyStore) TryClaim(key string, ttl time.Duration) (bool, error) {
	if err := validateClaimInputs(key, ttl); err != nil {
		return false, err
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	if _, exists := store.claimed[key]; exists {
		return false, nil
	}

	store.claimed[key] = struct{}{}
	return true, nil
}

// Compile-time check that InMemoryIdempotencyStore implements IdempotencyStore.
var _ IdempotencyStore = (*InMemoryIdempotencyStore)(nil)
