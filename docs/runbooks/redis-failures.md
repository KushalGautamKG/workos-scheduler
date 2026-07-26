# Runbook: Redis Failures

## Symptoms

- Alert `RedisIdempotencyFailures`.
- Rising `kernelq_redis_idempotency_errors_total` or worker idempotency error stats.
- Unexpected duplicate executions if claims fail open/closed incorrectly.

## Likely causes

- Redis unreachable, auth/TLS misconfig, or timeout.
- Wrong namespace / key prefix.
- Resource exhaustion on Redis.

## Immediate checks

### Metrics

- `rate(kernelq_redis_idempotency_errors_total[5m])`
- Claim success vs error rates when exposed
- Worker availability (indirect — workers may still run without Redis if backend disabled)

### Logs

- Idempotency claim failures with `error_type` (no Redis URLs with credentials).
- Confirm `KERNELQ_WORKER_IDEMPOTENCY_BACKEND` expectation (`redis` vs `memory` / disabled).

### Traces

- Spans around claim/execute may show errors; correlate with `job_id` / `trace_id`.

## Recovery actions

1. Verify Redis process/connectivity from the worker environment (without logging secrets).
2. If Redis is down and policy allows, temporarily switch to memory/disabled **only in non-production** after understanding duplicate risk.
3. Restore Redis; confirm claims succeed again.
4. Do not flush idempotency keys casually — that can allow duplicate side effects.

## Escalation guidance

- Escalate if Redis errors persist >15m in an environment that requires redis-backed dedupe.

## Rollback considerations

- Revert Redis client/config changes that coincide with the incident.

## Verification after recovery

- Idempotency error rate returns to ~0.
- Duplicate-skip behavior works for intentional replays (smoke scripts when appropriate).
- Alert clears.
