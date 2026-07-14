# Day 114 — Kafka Execution Replay Smoke

Concise note for duplicate Kafka dispatch → Redis execution claim.

## Methodology

1. Start Redis + Kafka; ensure `kernelq.jobs.dispatch` exists.
2. Run `cmd/consumer` with `KERNELQ_WORKER_IDEMPOTENCY_BACKEND=redis`, a **unique** consumer group, and `KERNELQ_KAFKA_AUTO_OFFSET_RESET=latest` (only the smoke’s messages are consumed).
3. Publish the **same** dispatch JSON **twice** (identical `job_id`, `attempt`, `payload`).
4. Stop the worker with `SIGINT`; assert shutdown counters.

Harness: `./worker/scripts/smoke_kafka_execution_replay.sh`

## Duplicate Kafka publish

At-least-once delivery (or an explicit double produce) can present the same logical attempt twice on `kernelq.jobs.dispatch`. Day 114 reproduces that by producing one event body twice before asserting behavior.

## Redis execution claim

Before `Executor.Execute`, the handler claims `execution:<job_id>:<attempt>` via Redis `SET NX EX`. First claim runs the executor; second claim skips (`event=duplicate_worker_execution`, `duplicate_skipped`) — not a DLQ error.

## Observed behavior

| Metric | Expected |
|--------|----------|
| `executor_calls` | `1` |
| `duplicate_executions` | `1` |
| `messages_processed` / `processed_messages` | `2` |
| `idempotency_errors` | `0` |

PASS line: `event=smoke_kafka_execution_replay success=true`

Duplicate replay is **expected** under at-least-once Kafka, not a failure.

## Remaining limitation

**Crash after claim, before result publish** is still future work: the Redis key may stay claimed while no result is written, so a later redispatch of the same attempt can skip execution incorrectly until TTL expiry or reconciliation.
