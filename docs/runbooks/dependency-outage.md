# Runbook: Dependency Outage

## Symptoms

- Redis / Kafka / gRPC errors in logs with `error_type` and `operation`.
- Rising recovery failure metrics for a dependency.
- Publish/consume stalls or claim failures.

## Possible causes

- Broker or Redis process down / network partition.
- Misconfigured addresses or credentials (never log secrets).
- gRPC target not listening; deadline too aggressive.

## Immediate safety checks

1. Identify which dependency failed (redis vs kafka vs grpc).
2. Confirm KernelQ does **not** bypass idempotency when Redis is down.
3. Distinguish dependency errors from application bugs (classification in logs).

## Relevant metrics

- `kernelq_recovery_*{dependency=...}`
- `kernelq:publish_success_rate`
- Redis idempotency error rates (when exposed)
- Kafka consume/publish rates

## Relevant structured logs

- `job claim failed` / `result publish failed`
- gRPC client dial/deadline errors with `operation` + `status`

## Relevant traces

- Span status Error on claim/publish/RPC
- Child spans missing when dependency unreachable

## Recovery actions

### Redis

1. Restore Redis; verify ping.
2. Retry/redeliver work; confirm claims succeed.
3. Never silently skip TryClaim.

### Kafka

1. Restore brokers; confirm topic list.
2. Retry publish; restart consumers if needed.
3. Measure observed recovery time (no production SLA claim).

### gRPC

1. Restore target; retry with bounded deadline.
2. Confirm no infinite client retry loops.

## Data-correctness checks

- No duplicate business completions after restore.
- Results not marked success if publish still failing.

## Escalation criteria

- Dependency down >30m during active load, or incorrect bypass of idempotency.

## Post-recovery validation

- Dependency health OK; recovery_success increments; SLI panels recover.
