package worker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeRedisSetNXClient records SetNX calls for unit tests (no real Redis).
type fakeRedisSetNXClient struct {
	keys       map[string]time.Duration
	lastKey    string
	lastValue  string
	lastTTL    time.Duration
	forceError error
}

func newFakeRedisSetNXClient() *fakeRedisSetNXClient {
	return &fakeRedisSetNXClient{
		keys: make(map[string]time.Duration),
	}
}

func (fake *fakeRedisSetNXClient) SetNX(
	_ context.Context,
	key string,
	value string,
	expiration time.Duration,
) (bool, error) {
	fake.lastKey = key
	fake.lastValue = value
	fake.lastTTL = expiration

	if fake.forceError != nil {
		return false, fake.forceError
	}
	if _, exists := fake.keys[key]; exists {
		return false, nil
	}
	fake.keys[key] = expiration
	return true, nil
}

func TestRedisIdempotencyFirstClaimReturnsTrue(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	claimed, err := store.TryClaim("execution:job-a:0", time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed {
		t.Fatal("expected first claim to return true")
	}
}

func TestRedisIdempotencyDuplicateClaimReturnsFalse(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	key := "execution:job-a:0"
	first, err := store.TryClaim(key, time.Hour)
	if err != nil || !first {
		t.Fatalf("first claim: claimed=%v err=%v", first, err)
	}

	second, err := store.TryClaim(key, time.Hour)
	if err != nil {
		t.Fatalf("second claim error: %v", err)
	}
	if second {
		t.Fatal("expected duplicate claim to return false")
	}
}

func TestRedisIdempotencyKeyIsNamespaced(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	_, err = store.TryClaim("execution:job-a:0", time.Hour)
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}

	want := "kernelq:idempotency:execution:job-a:0"
	if fake.lastKey != want {
		t.Fatalf("redis key = %q, want %q", fake.lastKey, want)
	}
}

func TestRedisIdempotencyReceivesValueOne(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	_, err = store.TryClaim("execution:job-a:0", time.Hour)
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if fake.lastValue != "1" {
		t.Fatalf("value = %q, want %q", fake.lastValue, "1")
	}
}

func TestRedisIdempotencyReceivesRequestedTTL(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	ttl := 42 * time.Second
	_, err = store.TryClaim("execution:job-a:0", ttl)
	if err != nil {
		t.Fatalf("TryClaim: %v", err)
	}
	if fake.lastTTL != ttl {
		t.Fatalf("ttl = %v, want %v", fake.lastTTL, ttl)
	}
}

func TestRedisIdempotencyEmptyKeyReturnsError(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	claimed, err := store.TryClaim("  ", time.Hour)
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	if claimed {
		t.Fatal("expected claimed=false")
	}
}

func TestRedisIdempotencyNonPositiveTTLReturnsError(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	claimed, err := store.TryClaim("execution:job-a:0", 0)
	if err == nil {
		t.Fatal("expected error for non-positive ttl")
	}
	if claimed {
		t.Fatal("expected claimed=false")
	}
}

func TestRedisIdempotencyNilClientConstructorReturnsError(t *testing.T) {
	_, err := NewRedisIdempotencyStore(nil, "kernelq:idempotency")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestRedisIdempotencyEmptyNamespaceConstructorReturnsError(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	_, err := NewRedisIdempotencyStore(fake, "  ")
	if err == nil {
		t.Fatal("expected error for empty namespace")
	}
}

func TestRedisIdempotencyClientErrorPropagates(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	fake.forceError = errors.New("redis unavailable")
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	claimed, err := store.TryClaim("execution:job-a:0", time.Hour)
	if err == nil {
		t.Fatal("expected redis client error")
	}
	if claimed {
		t.Fatal("expected claimed=false on error")
	}
	if !errors.Is(err, fake.forceError) && err.Error() != fake.forceError.Error() {
		t.Fatalf("error = %v, want %v", err, fake.forceError)
	}
}

func TestRedisIdempotencyImplementsInterface(t *testing.T) {
	fake := newFakeRedisSetNXClient()
	store, err := NewRedisIdempotencyStore(fake, "kernelq:idempotency")
	if err != nil {
		t.Fatalf("constructor: %v", err)
	}

	var iface IdempotencyStore = store
	claimed, err := iface.TryClaim("execution:iface:0", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed {
		t.Fatal("expected first claim via interface to succeed")
	}
}
