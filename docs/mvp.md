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
- **Worker result publishing** — `WorkerResultEvent` on `kernelq.jobs.results`
- **Python result consumer skeleton** — `KafkaResultConsumer` + `ResultStateHandler` (one-shot poll)
- **Postgres state updates from worker results** — `succeeded`, `retryable_failure`, `terminal_failure` mapping
- **Retry scheduling** — `retryable_failure` → `RETRY_SCHEDULED` when budget remains
- **Retry requeue scanner** — due `retry_scheduled` rows → `queued`
- **Retry exhaustion** — `retry_count >= max_retries` → `DEAD_LETTERED`
- **Dead-lettered job inspection** — `list_dead_lettered_jobs.py`
- **Manual dead-letter requeue** — operator moves `DEAD_LETTERED` → `QUEUED` (`retry_count` preserved)
- **Job state metrics snapshot** — `count_jobs_by_state` via CLI (`job_state_snapshot.py`) and **`GET /metrics/jobs`**
- **Prometheus-style job state metrics endpoint** — **`GET /metrics/prometheus`** (text exposition; scrape target only)
- **Smoke tests** — success, retry requeue, and exhaustion paths

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
- **Observability** — **`GET /metrics/prometheus`** returns Prometheus **text** metrics for job states; **no Prometheus server or Grafana yet** (see `docs/perf.md` for planned metrics)
- **Security** — no auth, no multi-user tenancy enforcement beyond `tenant_id` on rows
- **Deployment** — no Kubernetes / Helm; local Docker Compose only
- **Retry backoff** — basic fixed delay (`retry_after`); no exponential backoff + jitter
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
