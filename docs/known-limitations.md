# Known Limitations (Day 130)

Documenting limitations improves credibility: reviewers can trust what *is* verified when gaps are explicit.

## Execution recovery gap

```
Redis claim succeeds
  → worker crashes or result publication fails
  → replay of the same job_id+attempt may be skipped until TTL or reconciliation
```

**Evidence:** [execution-recovery.md](design/execution-recovery.md), [day114-kafka-execution-replay.md](benchmarks/day114-kafka-execution-replay.md), [day129-resilience.md](benchmarks/day129-resilience.md).

**Recommended future solution:**

- Execution leases with heartbeats
- Watchdog / reconciliation scanner
- Durable execution ownership distinct from “claim = done”

Day 130 does **not** redesign this model.

## Kafka pause/resume

KernelQ has:

- Queue-depth **visibility**
- Local **throttling** at the bounded work queue
- An **in-memory** `PauseResumeController` boundary driven by watermark policy

It does **not** claim production Kafka broker `Pause`/`Resume` of assigned partitions as a fully wired, cluster-proven control loop. Treat broker-level pause as a future integration on top of the existing policy interface.

## Production integrations (honest)

| Area | Reality |
|------|---------|
| EKS workflow | Validated **offline** (kustomize, dry-run scripts) unless a live cluster deploy was separately performed |
| CloudWatch | Configuration validated **offline**; no Day 130 claim of real ingestion |
| Prometheus / Grafana rules | Validated without a production managed install |
| Production SLO compliance | **No evidence** — proposed engineering targets only |
| Multi-region | **Not implemented** |
| AZ failure testing | **Not performed** in production |
| Production-scale soak | **Not performed** |
| Full transaction spanning business side effect + result publication | **Not implemented** (execute success and publish success remain distinct) |

## What remains strong despite these gaps

- Happy-path and retry/DLQ smokes
- Three-layer idempotency with replay smoke
- Observability contracts (metrics, traces, structured logs, alerts/runbooks)
- Container and Kubernetes policy foundation
- Deterministic local resilience testing

See [production-readiness.md](production-readiness.md) for the checklist view.
