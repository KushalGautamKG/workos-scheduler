# Runbook: High Error Rate

## Symptoms

- Alerts `HighFailureRate`, `RetryStorm`, or `PublishFailures`.
- Rising `kernelq:job_failure_rate` or falling `kernelq:publish_success_rate` / elevated `kernelq:retry_rate`.

## Likely causes

- Executor / dependency failures (retryable vs terminal).
- Result publish path failures (Kafka producer timeouts).
- Bad deploy or config regression.
- Retry storm from a poison subset of jobs (not the same as duplicate/idempotency skips).

## Immediate checks

### Metrics

- `kernelq:job_failure_rate`, `kernelq:job_success_rate`, `kernelq:retry_rate`
- `kernelq:publish_success_rate`
- Top worker errors panel (`kernelq_worker_errors_total` by `error_type`)
- Jobs by state gauges (`failed`, `retry_scheduled`, `dead_lettered`)

### Logs

- Messages: `job execution failed`, `result publish failed` with `error_type`, `operation`, `status`.
- Do **not** expect full payloads or secrets in logs (Day 127 policy).

### Traces

- Failed execute/publish spans should record errors; correlate via `trace_id` in logs.

## Recovery actions

1. Classify: execution failure vs publish failure vs retry amplification.
2. For publish failures: check Kafka connectivity and producer errors; republish path only after broker health recovers.
3. For execution failures: inspect recent code/config; fix poison input pattern if known.
4. For retry storms: confirm retry scanner / max_retries policy; avoid manual mass requeue until root cause is clear.
5. Treat duplicate_skipped / idempotency as expected — not an application crash.

## Escalation guidance

- Escalate if failure rate stays >5% for >30m, or publish success stays <99% while Kafka is up.

## Rollback considerations

- Roll back the last worker or control-plane change if the error onset matches a deploy.

## Verification after recovery

- Success rate recovers toward the 99% engineering target.
- Retry rate declines; publish success ≥ 99%.
- Related alerts clear.
