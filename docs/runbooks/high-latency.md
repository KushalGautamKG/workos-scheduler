# Runbook: High Latency

## Symptoms

- Alert `HighExecutionLatency` firing, or dashboard shows elevated `kernelq:execution_latency_p95` / `kernelq:queue_latency_p95`.
- Jobs take longer than the proposed P95 targets (execution < 2s, queue < 5s — engineering targets only).

## Likely causes

- Worker pool saturation or backpressure pause.
- Kafka consumer lag / under-provisioned workers.
- Slow dependencies (Redis, Postgres) or local resource contention.
- Hot tenants or oversized payloads (do not log full payloads).

## Immediate checks

### Metrics

- `kernelq:execution_latency_p95`, `kernelq:queue_latency_p95`
- `kernelq:queue_depth`, `kernelq:jobs_per_second`
- Worker availability `kernelq:worker_availability`
- Redis error rate if present

### Logs

- Structured worker logs: `operation=execute`, `status`, `job_id`, `trace_id` (no payloads).
- Look for WARN backpressure / queue-full events.

### Traces

- Follow `trace_id` across `kafka.process` → `worker.execute` → `kafka.publish`.
- Identify which span dominates duration.

## Recovery actions

1. Scale worker replicas if availability is healthy but queue depth grows.
2. Confirm Kafka brokers and consumer group are progressing.
3. Reduce load generation / pause non-essential producers in shared dev.
4. Restart unhealthy worker Pods only after checking recent deploy changes.

## Escalation guidance

- Escalate if P95 stays above objective for >30m after scaling, or if latency coincides with rising failure rate.

## Rollback considerations

- If latency started after a deploy, roll back the worker/control-plane image using the existing EKS rollback procedure (when applicable). Prefer dry-run awareness in prep environments.

## Verification after recovery

- `kernelq:execution_latency_p95` and `kernelq:queue_latency_p95` return under targets.
- Queue depth stabilizes or declines.
- Alert clears for a full evaluation window.
