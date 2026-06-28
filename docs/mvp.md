# KernelQ MVP Checkpoint

A beginner-friendly snapshot of what KernelQ does today — useful for demos, onboarding, and interviews.

---

## 1. What KernelQ Is

**KernelQ** is a **distributed job orchestration prototype**. It splits responsibility between a **Python control plane** and a **Go worker plane**, with **Kafka** between them and **Postgres** as the durable system of record.

| Plane | Role |
|-------|------|
| **Python control plane** | API, scheduling, Postgres job state, result reconciliation, retry policy |
| **Go worker plane** | Consumes dispatch messages, executes jobs, publishes results, routes poison messages to DLQ |
| **Kafka** | Connects the planes (`kernelq.jobs.dispatch`, `kernelq.jobs.results`, `kernelq.jobs.dlq`) |
| **Postgres** | Authoritative job lifecycle (`queued`, `dispatched`, `succeeded`, `retry_scheduled`, `dead_lettered`, …) |

**Interview sound bite:** *“Control plane owns policy and Postgres; workers own execution; Kafka carries dispatch and results.”*

---

## 2. MVP Capabilities

What works today (local / dev):

- **FastAPI** — job enqueue, read, cancel, retry basics
- **Postgres job persistence** — durable rows survive restarts
- **Scheduler prototypes** — FIFO, priority, weighted round-robin, composed pipeline
- **Atomic scheduler job claiming** — `FOR UPDATE SKIP LOCKED` avoids duplicate dispatch
- **Kafka dispatch publishing** — scheduler tick publishes `DispatchEvent` to `kernelq.jobs.dispatch`
- **Go Kafka worker consumer** — poll loop, message validation, execution handler
- **Go worker pool** — configurable concurrent executors (**default 4**); consumer reads Kafka, workers run jobs in parallel (`worker/internal/worker/worker_pool.go`)
- **Bounded worker work queue** — **Day 87** evaluates depth vs high/low watermarks; **`event=worker_backpressure_pause`** / **`event=worker_backpressure_resume`**; **`backpressure_pause_events`** / **`backpressure_resume_events`** at shutdown—in-memory controller; real Kafka pause/resume future; **Day 82** backoff default ([design doc](design/kafka-pause-resume-backpressure.md))
- **Worker result publishing** — `WorkerResultEvent` on `kernelq.jobs.results`
- **Python result consumer skeleton** — `KafkaResultConsumer` + `ResultStateHandler` (one-shot poll)
- **Postgres state updates from worker results** — `succeeded`, `retryable_failure`, `terminal_failure` mapping
- **Retry scheduling** — `retryable_failure` → `RETRY_SCHEDULED` when budget remains
- **Retry requeue scanner** — due `retry_scheduled` rows → `queued`
- **Retry exhaustion** — `retry_count >= max_retries` → `DEAD_LETTERED`
- **Dead-lettered job inspection** — `list_dead_lettered_jobs.py`
- **Manual dead-letter requeue** — operator moves `DEAD_LETTERED` → `QUEUED` (`retry_count` preserved)
- **Job state metrics snapshot** — `count_jobs_by_state` via CLI (`job_state_snapshot.py`) and **`GET /metrics/jobs`**
- **Job duration metrics** — queue wait **p50/p95/p99** from **`dispatched_at`** (`job_duration_snapshot.py`, **`GET /metrics/durations`**; also **`kernelq_queue_wait_seconds`** on **`GET /metrics/prometheus`** — gauge quantiles, not histograms yet)
- **Latency metrics seed script** — **`seed_latency_metrics.py`** populates succeeded jobs with realistic queue waits for local testing and benchmarking
- **Load job generator** — **`generate_load_jobs.py`** creates **`queued`** benchmark workloads (unique **`--prefix`** for cleanup); dispatch via **`run_scheduler_tick_once.py`**
- **Scheduler throughput benchmark** — **`benchmark_scheduler_throughput.py`** measures **`queued` → `dispatched`** dispatch rate; **`--trials`** for min/avg/max throughput (local baseline, not production claims)
- **Benchmark baseline docs** — **[benchmarks/day75-baseline.md](benchmarks/day75-baseline.md)** (early local results); **[benchmarks/day77-scheduler-1000.md](benchmarks/day77-scheduler-1000.md)** (1000-job scheduler benchmark, 3 trials)—local baselines, not production claims
- **Prometheus-style metrics endpoint** — **`GET /metrics/prometheus`** (job state counts + queue wait quantile gauges)
- **Prometheus scrape configuration example** — `infra/prometheus/prometheus.yml` (15s scrape of `/metrics/prometheus`; see `docs/deploy.md`)
- **Local Prometheus service** — `docker compose up -d prometheus` (UI on `:9090`)
- **Local Grafana service and starter dashboard** — `docker compose up -d grafana` (UI on `:3000`; **KernelQ MVP** charts `kernelq_jobs_by_state`)
- **Structured script logs** — one-shot scripts print `event=<name>` summary lines for operator/debug visibility (complements Prometheus)
- **Smoke tests** — success, retry requeue, exhaustion, queue-wait latency (`smoke_queue_wait_metrics.sh`), and worker queue saturation (`smoke_queue_saturation.sh`)

---

## 3. Supported Flows

### Success path

```
QUEUED → DISPATCHED → (worker executes) → SUCCEEDED
```

Scheduler claims a queued job, publishes to Kafka, worker runs and reports `succeeded`, control plane updates Postgres.

### Retry path

```
DISPATCHED → (retryable_failure) → RETRY_SCHEDULED → QUEUED → DISPATCHED
```

Transient failure schedules a retry with `retry_after`; **RetryScanner** requeues when due; scheduler dispatches again.

### Exhaustion path

```
DISPATCHED → (retryable_failure, budget exhausted) → DEAD_LETTERED
```

No more automatic retries — terminal state for operator inspection.

### Manual recovery

```
DEAD_LETTERED → (operator requeue) → QUEUED → DISPATCHED → …
```

After fixing root cause, ops manually requeue; **scheduler** picks up the job on the next tick.

---

## 4. How To Demo

From the **repository root**. Requires Docker, Python 3, and Go (for full completion smoke).

**1. Start infra**

```bash
docker compose up -d postgres zookeeper kafka
```

**2. Create Kafka topics**

```bash
./infra/kafka/create-topics.sh
```

**3. Run smoke tests**

| Script | What it proves |
|--------|----------------|
| `./control_plane/scripts/smoke_full_completion.sh` | End-to-end: queued → dispatch → worker → result → **succeeded** |
| `./control_plane/scripts/smoke_retry_requeue.sh` | **retryable_failure** → `retry_scheduled` → `queued` → `dispatched` (no Go worker for retry inject) |
| `./control_plane/scripts/smoke_retry_exhaustion.sh` | Exhausted retries → **dead_lettered**; scanner does not requeue |
| `./control_plane/scripts/smoke_queue_wait_metrics.sh` | Non-zero queue wait from **`dispatched_at`** (`queue_wait_seconds > 0`) |
| `./worker/scripts/smoke_queue_saturation.sh` | Bounded worker queue saturation (**`work_queue_full_errors > 0`**); **`event=smoke_worker_queue_saturation success=true`** (no Kafka) |

Each smoke script prints a structured **`event=smoke_*`** summary line (`success=true|false`) at the end — grep these to verify demo success quickly:

```bash
./control_plane/scripts/smoke_full_completion.sh 2>&1 | tee demo.log
grep "event=smoke_" demo.log
```

**4. Job state snapshot** (counts by Postgres state — useful after smoke tests)

```bash
PYTHONPATH=. python3 control_plane/scripts/job_state_snapshot.py
```

**5. Inspect dead-lettered jobs**

```bash
PYTHONPATH=. python3 control_plane/scripts/list_dead_lettered_jobs.py
```

**Optional — manual requeue after inspection**

```bash
PYTHONPATH=. python3 control_plane/scripts/requeue_dead_lettered_job.py <job_id>
PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py
```

---

## 5. Test Commands

**MVP smoke scripts** (see §4) emit **`event=smoke_*`** summary lines — useful for a quick pass/fail check after a demo or CI run (`grep "event=smoke_"`).

**Python (control plane)**

```bash
python3 -m pytest control_plane/tests
```

Repository integration tests need Postgres: `docker compose up -d postgres`.

**Go (worker)**

```bash
cd worker && go test ./...
```

---

## 6. Known Limitations

Honest gaps — not production-ready yet:

- **Result consumer** — one-shot poll (`consume_result_once.py`), not a long-running daemon
- **Scheduler** — one-shot tick (`run_scheduler_tick_once.py`), not a continuous loop
- **Prometheus** — local Docker Compose service only; no production TSDB or HA deployment
- **Grafana dashboard** — minimal **KernelQ MVP** panel; only **`kernelq_jobs_by_state`** gauges for now
- **Duration metrics** — Postgres snapshot **p50/p95/p99** queue wait on **`/metrics/durations`** and **`/metrics/prometheus`**; not native Prometheus histograms yet
- **Structured logs** — one-shot scripts only; no centralized log aggregation yet
- **Security** — no auth, no multi-user tenancy enforcement beyond `tenant_id` on rows
- **Deployment** — no Kubernetes / Helm; local Docker Compose only
- **Worker Kafka backpressure** — **Day 87** watermark evaluation + pause/resume events/stats (in-memory); **Day 82** backoff default in **`cmd/consumer`**; real Kafka partition pause/resume future ([`docs/design/kafka-pause-resume-backpressure.md`](design/kafka-pause-resume-backpressure.md))
- **Delivery semantics** — no exactly-once guarantees; at-least-once with idempotency left to callers
- **Config / secrets** — no production-grade secret management or env-based config layering

---

## 7. Resume Talking Points

Use these in interviews or README intros:

- Built a **Python control plane + Go worker plane** split with clear contracts
- **Kafka-based dispatch and result topics** — workers publish structured `WorkerResultEvent` JSON
- **Postgres-backed lifecycle state machine** — `queued`, `dispatched`, `succeeded`, `retry_scheduled`, `dead_lettered`
- **Retry / requeue / dead-letter lifecycle** — bounded retries, scanner requeue, terminal `DEAD_LETTERED`, manual ops requeue
- **Smoke-tested distributed feedback loop** — scheduler → Kafka → worker → results → Postgres update
- **Atomic job claiming** and **DLQ routing** for invalid Kafka messages on the worker side

**One-liner:** *“KernelQ is a job orchestration MVP: Postgres state, Kafka messaging, Python scheduling, Go execution, with retry and dead-letter paths proven by smoke tests.”*
