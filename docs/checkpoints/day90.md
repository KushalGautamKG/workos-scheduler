# KernelQ Day 90 Checkpoint

---

## 1. Summary

KernelQ has **passed MVP**. The system now demonstrates a **complete distributed job loop**: Postgres-backed scheduling, Kafka dispatch and results, Go worker execution, retry and dead-letter paths, and operator-facing observability—all proven locally with smoke tests and automated test suites.

From Day 90 onward, the project enters a **production-readiness / FAANG systems-project phase**. The focus shifts from “does the loop work?” to **throughput measurement, idempotency, internal service boundaries, distributed tracing, and cloud deployment**—the layers that separate a working prototype from something you can defend in a senior backend or platform interview.

**Interview sound bite:** *“MVP proved the lifecycle loop; post-MVP is about benchmarks, dedupe, gRPC/OTel, and EKS—making the architecture operable at scale.”*

---

## 2. Completed Capabilities

### Control Plane

- **Python FastAPI** control plane — job enqueue, read, cancel, retry, state-transition enforcement
- **Repository layer** — parameterized SQL over Postgres; API handlers stay free of raw query strings
- **Result consumer skeleton** — parse/validate `WorkerResultEvent`, map outcomes to Postgres via `ResultStateHandler`
- **Structured script logs** — grep-friendly `event=<name>` summary lines on one-shot ops scripts

### Scheduler

- **In-memory policy prototypes** — FIFO, priority, weighted round-robin, composed pipeline (simulation + metrics)
- **Postgres-backed schedulable queries** — `queued` jobs ordered by `priority DESC`, `created_at ASC`
- **Atomic job claiming** — `FOR UPDATE SKIP LOCKED` in one transaction (`queued` → `dispatched`)
- **Scheduler tick runner** — batch claim + optional Kafka publish per tick
- **Composite index** — `idx_jobs_state_priority_created_at` aligned with claim query shape

### Persistence

- **PostgreSQL durable job state** — authoritative lifecycle (`queued`, `dispatched`, `succeeded`, `retry_scheduled`, `dead_lettered`, …)
- **Strict state machine** — shared `job_state.py` rules enforced in API (409 on illegal transitions)
- **Retry metadata** — `retry_count`, `max_retries`, `retry_after` on rows
- **Dispatch timestamps** — `dispatched_at` for queue-wait and duration metrics

### Kafka

- **Four-topic layout** — `kernelq.jobs.dispatch`, `kernelq.jobs.results`, `kernelq.jobs.retry`, `kernelq.jobs.dlq`
- **Python dispatch producer** — `DispatchEvent` JSON after Postgres claim
- **Go result producer** — `WorkerResultEvent` JSON after execution
- **Go DLQ producer** — `DeadLetterEvent` for invalid dispatch messages
- **Cross-language contract** — Python publishes, Go parses/validates the same dispatch shape

### Worker Plane

- **Go worker plane** — Kafka poll loop, message validation, handler/executor layering
- **Concurrent worker pool** — configurable goroutine pool (default **4** workers)
- **Bounded worker queue** — buffered channel caps in-flight work (default capacity **100**)
- **Result and DLQ routing** — publish outcomes and poison messages without stopping the poll loop
- **Graceful shutdown** — SIGINT/SIGTERM via `context.Context`; shutdown stats on exit

### Retry / DLQ

- **Retry scheduling** — `retryable_failure` → `retry_scheduled` when budget remains
- **Retry requeue scanner** — due `retry_scheduled` rows → `queued`
- **Retry exhaustion** — `retry_count >= max_retries` → `dead_lettered`
- **Manual dead-letter recovery** — operator requeue `dead_lettered` → `queued`
- **Kafka DLQ** — invalid dispatch messages preserved on `kernelq.jobs.dlq`

### Observability

- **Job state metrics** — `count_jobs_by_state` via CLI and `GET /metrics/jobs`
- **Duration metrics** — queue wait p50/p95/p99 from `dispatched_at` (`GET /metrics/durations`)
- **Prometheus / Grafana** — `GET /metrics/prometheus`, local Docker Compose stack, **KernelQ MVP** dashboard (`kernelq_jobs_by_state`)
- **Consumer shutdown stats** — messages seen/processed, DLQ counts, work-queue counters

### Benchmarking

- **Load job generator** — seed `queued` rows for scheduler experiments
- **Scheduler throughput benchmark** — `benchmark_scheduler_throughput.py` with multi-trial min/avg/max
- **Benchmark reports** — [Day 75 baseline](../benchmarks/day75-baseline.md), [Day 77 scheduler 1000-job](../benchmarks/day77-scheduler-1000.md) (local dev baselines)

### Backpressure

- **Queue saturation stats** — `work_queue_full_errors`, `work_items_enqueued`, `work_queue_capacity`
- **Queue depth** — point-in-time `work_queue_depth` at shutdown and during policy evaluation
- **Local backoff** — 50ms sleep + one retry on bounded-queue full (Day 82)
- **High/low watermark policy** — `BackpressurePolicy` with hysteresis (default 80% / 50%)
- **Pause/resume controller boundary** — `InMemoryPauseResumeController` for tests; pause/resume event counters
- **Env-configurable backpressure** — `KERNELQ_WORKER_BACKPRESSURE_*` env vars (disabled by default; EKS ConfigMaps path later)

---

## 3. Current Limitations

Honest gaps between “MVP proven” and “production ready”:

| Area | Status |
|------|--------|
| **Redis dedupe** | Not implemented — no idempotency / exactly-once handoff layer yet |
| **Real Kafka pause/resume** | Policy + in-memory controller only — broker `Pause`/`Resume` API not wired |
| **gRPC internal services** | Not implemented — planes communicate via Kafka + REST today |
| **OpenTelemetry traces** | Not implemented — logs and Prometheus gauges only |
| **Kubernetes / EKS** | Local Docker Compose only — no Helm, no cloud deployment |
| **CloudWatch alerts** | Not implemented — no AWS ops integration |
| **Worker throughput benchmark** | Pending — current reports measure scheduler claim rate, not Go execution |
| **End-to-end benchmark** | Pending — enqueue → dispatch → worker → result → terminal state throughput |

Additional MVP carryovers: result consumer and scheduler are **one-shot scripts**, not long-running daemons; Prometheus uses **snapshot quantiles**, not histogram buckets; no auth or multi-tenant enforcement beyond row-level `tenant_id`.

---

## 4. Evidence So Far

| Evidence | Result |
|----------|--------|
| **Python tests** | **222 passing** (`python3 -m pytest control_plane/tests`) |
| **Go tests** | **Passing** (`cd worker && go test ./...`) |
| **Day 75 benchmark baseline** | Documented local scheduler/load insertion baseline |
| **Day 77 scheduler 1000-job benchmark** | 3-trial `queued` → `dispatched` throughput report |
| **Worker queue saturation smoke** | `./worker/scripts/smoke_queue_saturation.sh` — `work_queue_full_errors > 0` |
| **Backpressure config smoke** | `./worker/scripts/smoke_backpressure_config.sh` — startup env lines verified |
| **Full completion smoke** | `./control_plane/scripts/smoke_full_completion.sh` — end-to-end success path |
| **Retry / exhaustion smokes** | Requeue and dead-letter exhaustion paths validated |

These artifacts support interview narratives with **reproducible commands**, not hand-wavy claims.

---

## 5. Next Phase Roadmap

| Days | Focus |
|------|-------|
| **91–95** | **Worker throughput benchmark** and **end-to-end benchmark** — measure dispatch-to-consume, execution, and completion rates; close the gap between scheduler-only numbers and real system capacity |
| **96–105** | **Redis idempotency / dedupe** — claim tokens, dispatch dedupe keys, at-least-once safety without double execution |
| **106–115** | **gRPC + OpenTelemetry** — internal service RPCs between control plane components; distributed traces across scheduler, Kafka, worker, and Postgres |
| **116–130** | **Docker / Kubernetes / AWS EKS / CloudWatch** — container images, Helm charts, ConfigMap-driven worker config (including backpressure env), production metrics and alerting |

Each phase builds on the MVP foundation without rewriting the core split: **Python decides, Go executes, Kafka transports, Postgres records truth**.

---

## 6. Resume Alignment

KernelQ is intentionally shaped as a **full-stack distributed systems portfolio project**. The completed MVP maps cleanly to skills interviewers probe for; the roadmap fills the remaining production gaps.

**Stack trajectory:**

| Layer | Technology |
|-------|------------|
| Control plane | **Python**, **FastAPI** |
| Worker plane | **Go** |
| Messaging | **Kafka** |
| Durable state | **PostgreSQL** |
| Caching / dedupe (planned) | **Redis** |
| Internal RPC (planned) | **gRPC** |
| Tracing (planned) | **OpenTelemetry** |
| Metrics | **Prometheus** |
| Dashboards | **Grafana** |
| Deployment (planned) | **AWS EKS**, **CloudWatch** |

**How to describe the project on a resume:**

- Built a **Python control plane + Go worker plane** job orchestrator with **Kafka** handoff and **Postgres** lifecycle state
- Implemented **atomic scheduling**, **retry/dead-letter** flows, and **DLQ routing** with smoke-tested correctness
- Added **worker backpressure** — bounded queue, watermark policy, env-configurable thresholds, pause/resume boundary
- Instrumented with **Prometheus/Grafana**, structured ops logs, and **documented benchmarks**

**One-liner:** *“KernelQ is a post-MVP distributed job platform: MVP loop proven at Day 90; next phase adds benchmarks, Redis dedupe, gRPC/OTel, and EKS deployment.”*

---

## Related Docs

- [MVP checkpoint](../mvp.md) — demo flows and smoke commands
- [Architecture](../architecture.md) — control/worker split and data flow
- [Performance](../perf.md) — metrics plan and benchmark methodology
- [Runbooks](../runbooks.md) — operational smoke tests and triage
- [Kafka pause/resume backpressure design](../design/kafka-pause-resume-backpressure.md)
