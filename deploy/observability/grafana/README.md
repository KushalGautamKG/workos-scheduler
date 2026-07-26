# KernelQ Grafana Dashboard (Day 128)

Dashboard file: [`kernelq-dashboard.json`](kernelq-dashboard.json)

Title: **KernelQ Monitoring**

Datasource references use the Grafana template variable `${datasource}` (type Prometheus).
Do **not** hardcode datasource UIDs or production Grafana/Prometheus URLs.

## Dashboard sections

| Section | Panels | Purpose |
|---------|--------|---------|
| **Overview** | Worker availability, queue depth, jobs/sec, success/failure | At-a-glance health |
| **Reliability** | Success / failure / retry rates; execution & queue P95 latency | SLO-oriented trends |
| **Kafka / Redis** | Publish rate, consume rate, Redis idempotency claims/errors | Messaging and dedupe path |
| **Errors** | Top worker errors by `error_type`; jobs by state (absolute) | Diagnosis + inventory |

## Why rates over raw counters

Counters only go up. **Rates** (`rate(...[5m])`) show intensity and change — the signal operators need for “is this getting worse?”. Absolute counters (and gauges like queue depth / jobs-by-state) still matter for magnitude and capacity; the dashboard shows **both**.

## Why percentile latency matters

Averages hide tail pain. **P95** (and eventually P99) answers whether most users are fine while a meaningful minority are slow. Recording rule `kernelq:execution_latency_p95` and control-plane `kernelq:queue_latency_p95` feed these panels.

## Metrics, logs, and traces

```
Metrics  →  Is the system healthy? (rates, SLIs, alerts)
Logs     →  What happened on a specific job/request?
Traces   →  Where did time go across gRPC / Kafka / execute?
```

Use metrics to detect and alert, logs (structured JSON, Day 127) for incident detail, and OpenTelemetry traces (Days 119–122) to correlate latency across boundaries via `trace_id` / `span_id` in logs.

## Offline validation

```bash
./worker/scripts/smoke_dashboard.sh
```

This validates JSON structure and panel queries. It does **not** prove a live Grafana instance or dashboard performance.
