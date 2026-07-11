package worker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInMemoryIdempotencyFirstClaimSucceeds(t *testing.T) {
	store := NewInMemoryIdempotencyStore()

	claimed, err := store.TryClaim("execution:job-a:0", time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed {
		t.Fatal("expected first claim to succeed")
	}
}

func TestInMemoryIdempotencyDuplicateClaimReturnsFalse(t *testing.T) {
	store := NewInMemoryIdempotencyStore()
	key := "execution:job-a:0"

	first, err := store.TryClaim(key, time.Hour)
	if err != nil {
		t.Fatalf("first claim error: %v", err)
	}
	if !first {
		t.Fatal("expected first claim to succeed")
	}

	second, err := store.TryClaim(key, time.Hour)
	if err != nil {
		t.Fatalf("second claim error: %v", err)
	}
	if second {
		t.Fatal("expected duplicate claim to return false")
	}
}

func TestInMemoryIdempotencyDifferentKeysSucceed(t *testing.T) {
	store := NewInMemoryIdempotencyStore()

	a, err := store.TryClaim("execution:job-a:0", time.Hour)
	if err != nil || !a {
		t.Fatalf("key a: claimed=%v err=%v", a, err)
	}

	b, err := store.TryClaim("execution:job-b:0", time.Hour)
	if err != nil || !b {
		t.Fatalf("key b: claimed=%v err=%v", b, err)
	}

	c, err := store.TryClaim("execution:job-a:1", time.Hour)
	if err != nil || !c {
		t.Fatalf("key c (different attempt): claimed=%v err=%v", c, err)
	}
}

func TestInMemoryIdempotencyEmptyKeyReturnsError(t *testing.T) {
	store := NewInMemoryIdempotencyStore()

	for _, key := range []string{"", "   "} {
		claimed, err := store.TryClaim(key, time.Hour)
		if err == nil {
			t.Fatalf("expected error for key %q", key)
		}
		if claimed {
			t.Fatalf("expected claimed=false for key %q", key)
		}
	}
}

func TestInMemoryIdempotencyNonPositiveTTLReturnsError(t *testing.T) {
	store := NewInMemoryIdempotencyStore()

	for _, ttl := range []time.Duration{0, -time.Second} {
		claimed, err := store.TryClaim("execution:job-a:0", ttl)
		if err == nil {
			t.Fatalf("expected error for ttl %v", ttl)
		}
		if claimed {
			t.Fatalf("expected claimed=false for ttl %v", ttl)
		}
	}
}

func TestInMemoryIdempotencyImplementsInterface(t *testing.T) {
	var store IdempotencyStore = NewInMemoryIdempotencyStore()
	claimed, err := store.TryClaim("execution:iface:0", time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !claimed {
		t.Fatal("expected first claim via interface to succeed")
	}
}

func TestInMemoryIdempotencyConcurrentDuplicateClaims(t *testing.T) {
	store := NewInMemoryIdempotencyStore()
	const goroutines = 64
	key := "execution:job-race:0"

	var successes atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			claimed, err := store.TryClaim(key, time.Hour)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if claimed {
				successes.Add(1)
			}
		}()
	}

	wg.Wait()

	if got := successes.Load(); got != 1 {
		t.Fatalf("expected exactly 1 successful claim, got %d", got)
	}
}
