package worker

import (
	"fmt"
	"strings"
	"time"
)

// IdempotencyStore answers: "May I process this logical event for the first time?"
//
// Callers (future DispatchEventHandler wiring) ask this before Execute — not
// "what is the job's durable state?" (that stays in Postgres).
//
// TryClaim mirrors Redis SET key value NX EX ttl in spirit:
//   - claimed == true  → first claimant; proceed with side effects
//   - claimed == false → duplicate while the key is still live; skip execution
//
// Redis is available via RedisIdempotencyStore (Day 111); handler wiring is still future.
type IdempotencyStore interface {
	TryClaim(key string, ttl time.Duration) (claimed bool, err error)
}

// validateClaimInputs rejects blank keys and non-positive TTLs before any
// store backend is touched. Every IdempotencyStore implementation should call this.
func validateClaimInputs(key string, ttl time.Duration) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("idempotency key must be non-empty")
	}
	if ttl <= 0 {
		return fmt.Errorf("idempotency ttl must be > 0, got %v", ttl)
	}
	return nil
}
