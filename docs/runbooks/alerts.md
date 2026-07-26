# Alert Runbook Index (Day 128)

Alerts are defined in `deploy/observability/prometheus/alert-rules.yaml`.
Each alert annotation `runbook:` should point here or to a sibling file.

| Alert | Severity | Runbook |
|-------|----------|---------|
| WorkerUnavailable | critical | [worker-unavailable.md](worker-unavailable.md) |
| HighExecutionLatency | warning | [high-latency.md](high-latency.md) |
| HighFailureRate | critical | [high-error-rate.md](high-error-rate.md) |
| RetryStorm | warning | [high-error-rate.md](high-error-rate.md) |
| PublishFailures | critical | [high-error-rate.md](high-error-rate.md) |
| KafkaConsumerStopped | critical | [kafka-backlog.md](kafka-backlog.md) |
| QueueBacklogGrowing | warning | [kafka-backlog.md](kafka-backlog.md) |
| RedisIdempotencyFailures | warning | [redis-failures.md](redis-failures.md) |

## How to use

1. Confirm the alert is still firing (sustained, not a blip).
2. Open the linked runbook.
3. Check **metrics → logs → traces** in that order unless the runbook says otherwise.
4. Apply recovery, then verify the alert clears and SLIs recover.

## Related

- Design: [docs/design/monitoring.md](../design/monitoring.md)
- Structured logging: [docs/design/structured-logging.md](../design/structured-logging.md)
- Operational overview: [docs/runbooks.md](../runbooks.md)
