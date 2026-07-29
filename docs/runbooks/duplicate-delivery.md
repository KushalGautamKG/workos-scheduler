# Runbook: Duplicate Delivery

## Symptoms

- Same `job_id`+`attempt` observed more than once (Kafka redelivery, replay smoke).
- Logs: `duplicate execution skipped` at WARN.
- Metric `kernelq_duplicate_deliveries_total` increments.
- Executor call count stays at one for that attempt.

## Possible causes

- Kafka at-least-once delivery (expected).
- Intentional replay / smoke tests.
- Consumer rebalance after crash.

## Immediate safety checks

1. Confirm this is **duplicate delivery**, not **duplicate completion**.
2. Confirm Redis/memory claim still held for the attempt.
3. Confirm worker did not crash while handling the duplicate.

## Relevant metrics

- `kernelq_duplicate_deliveries_total{outcome="skipped"}`
- Handler `duplicate_executions` shutdown stats
- Jobs/sec and success rate (should remain stable)

## Relevant structured logs

- `duplicate execution skipped` with `job_id`, `attempt`, `status=duplicate_skipped`
- Must **not** appear as fatal/crash

## Relevant traces

- Execution span marked duplicate (Day 120+ recording)

## Recovery actions

1. Usually none — this is expected at-least-once behavior.
2. If duplicate **completions** appear (two success results for same attempt), treat as **data-correctness incident**: freeze publishers, inspect idempotency store, escalate.

## Data-correctness checks

| OK | Incident |
|----|----------|
| Kafka delivery count ≥ 1 | Multiple completed executions for same attempt |
| Business execution count = 1 | Result topic shows two successes for same attempt |

## Escalation criteria

- Any confirmed duplicate **completion** in shared environments.

## Post-recovery validation

- Replay smoke / second Handle returns `duplicate_skipped`.
- Exactly one success result for the attempt.
