# Monitoring Design (Day 128)

## Pipeline

```
Application
     ↓
Metrics
     ↓
Recording Rules
     ↓
Alert Rules
     ↓
Dashboards
     ↓
Runbooks
```

Applications expose Prometheus metrics (control plane today; worker counters/histograms as instrumentation grows). **Recording rules** precompute stable SLI-shaped series. **Alert rules** fire on sustained conditions. **Dashboards** visualize rates, percentiles, and absolute inventory. **Runbooks** tell on-call what to do next.

## SLIs (primary)

| SLI | Definition |
|-----|------------|
| **Availability (job success)** | Successful job executions ÷ total execution attempts |
| **Execution latency** | 95th percentile execution duration |
| **Queue latency** | Time between enqueue and worker execution (P95) |
| **Retry rate** | Retries ÷ executions |
| **Result publish success** | Successful publishes ÷ publish attempts |
| **Worker health** | Healthy worker Pods ÷ desired worker Pods |

## Initial SLOs (engineering targets only)

These are **proposed starting objectives**. They are **not** production-validated commitments and should be refined with production telemetry.

| Objective | Target |
|-----------|--------|
| Job success | 99% |
| Execution latency P95 | < 2 seconds |
| Queue latency P95 | < 5 seconds |
| Publish success | 99% |
| Worker availability | 99.9% |

## Alert philosophy

- Alert on **sustained** conditions (`for:` windows), not single scrape spikes.
- Prefer **symptoms** users feel (failure rate, latency, no workers) over every internal counter.
- Every alert includes `severity` and a `runbook` annotation.
- Duplicate/idempotency behavior is expected — do not treat it as an outage by itself.

## Recording rules

Prefer named series such as `kernelq:job_success_rate` over repeating expensive PromQL in every panel and alert. Stable names are a contract between metrics, alerts, and dashboards.

Location: `deploy/observability/prometheus/recording-rules.yaml`

## Dashboard philosophy

- Show **rates** for change and intensity; **absolute** values for backlog and inventory.
- Prefer **percentiles** for latency.
- Use placeholder Prometheus datasources (`${datasource}`) — no hardcoded UIDs or production URLs.

Location: `deploy/observability/grafana/kernelq-dashboard.json`

## Runbook ownership

Alert → runbook path under `docs/runbooks/`. Owners keep symptoms, checks (metrics / logs / traces), recovery, escalation, and verification current as the platform evolves.

Index: [docs/runbooks/alerts.md](../runbooks/alerts.md)

## Future integrations

These are **future** integrations — not delivered or validated in Day 128:

- PagerDuty (or similar) notification routing
- CloudWatch Alarms
- Alertmanager
- Managed Prometheus (e.g. Amazon Managed Prometheus)
- Managed Grafana

Day 128 validates **configuration and contracts offline** only.
