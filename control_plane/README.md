# Control Plane

The KernelQ control plane is the "brain" of the system. It makes decisions about when jobs should run and coordinates everything.

## Why Python?

The control plane is written in Python because:

- **Fast development**: Python lets us build complex scheduling logic quickly
- **Rich ecosystem**: Great libraries for APIs, databases, and integrations
- **Readability**: Easy to understand and maintain coordination code
- **Flexibility**: Perfect for the decision-making and orchestration work

The control plane doesn't need to be super fast—it handles lower-frequency operations like API requests and scheduling decisions. The worker plane (written in Go) handles the high-speed task execution.

## Local Setup

Use these commands from the repository root.

1) Install dependencies:

```bash
python3 -m pip install -r control_plane/requirements.txt
```

2) Run all control-plane tests:

```bash
python3 -m pytest control_plane/tests
```

3) Start the FastAPI server:

```bash
python3 -m uvicorn control_plane.api:app --reload
```

4) View API docs:

- `http://127.0.0.1:8000/docs`

5) Health check:

```bash
curl http://127.0.0.1:8000/health
```

Note: this setup is local-only for now. Docker and cloud deployment will come later.

## Local Postgres

KernelQ includes a **local Postgres** service in the repo’s Docker Compose file. Postgres will hold **durable job state** so jobs survive restarts and can be shared across processes.

The first migration, `control_plane/migrations/001_create_jobs.sql`, creates the **`jobs`** table. **Wiring the FastAPI API to Postgres** comes in a later step.

From the repository root:

```bash
docker compose up -d postgres
```

Apply the migration:

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/migrations/001_create_jobs.sql
```

## Postgres Repository Layer

KernelQ now includes a small **Python repository layer** for the **`jobs`** table (`kernelq/job_repository.py` plus `kernelq/db.py` for connections).

It supports **create**, **fetch by id**, **update state**, and **delete**—so SQL stays in one place instead of spreading across API handlers.

**Repository tests** (`tests/test_job_repository.py`) need the **local Postgres container** running and the **migration** applied first.

**API integration** with this repository will be wired in a later step; the FastAPI app can still use in-memory state until then.

## Database-backed Scheduling Path

`JobRepository` can now **list queued jobs from Postgres** (`list_schedulable_jobs`) and **mark a queued job as dispatched** (`mark_job_dispatched`). That is the first step toward moving **scheduler selection** from in-memory prototypes to **durable, database-backed orchestration**—the same ordering ideas (priority, then age), but stored in the `jobs` table so they survive restarts. **Kafka publishing** will be added later; today this path updates Postgres only.

## Scheduler Tick Runner

KernelQ now has a **`SchedulerTickRunner`** (`kernelq/scheduler_tick.py`). Each **`run_once()`** tick calls **`claim_schedulable_jobs`** (up to `max_jobs_per_tick`), then optionally publishes **`DispatchEvent`** messages when a **`job_producer`** is passed in. It runs **synchronously** for now—no async yet.

## Atomic Job Claiming

Scheduler ticks claim work through **`JobRepository.claim_schedulable_jobs()`**: it **selects `queued` jobs and marks them `dispatched` in one Postgres transaction**, which **reduces duplicate dispatch risk** when multiple schedulers run. The SQL uses row locking with **`FOR UPDATE SKIP LOCKED`** so instances skip rows another scheduler is already claiming. Kafka publish runs **after** claim when a producer is configured.

## Inspecting Scheduler Query Plans

KernelQ includes **`control_plane/sql/explain_claim_schedulable_jobs.sql`** to inspect Postgres plans for scheduler queries. **`EXPLAIN`** shows how Postgres *plans* to run a query; **`EXPLAIN ANALYZE`** actually *runs* it and reports timing. That helps catch slow scheduler queries before load testing. From the repository root (Postgres running, migration applied):

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/explain_claim_schedulable_jobs.sql
```

## Scheduler Query Index

KernelQ now has a **scheduler-specific Postgres index** — **`idx_jobs_state_priority_created_at`** on `(state, priority DESC, created_at ASC)`. It supports querying **`queued`** jobs by **priority** (urgent first) and **age** (FIFO among equals), matching `claim_schedulable_jobs`. Apply locally with **migration 002**:

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/migrations/002_add_scheduler_claim_index.sql
```

Rerun the **EXPLAIN** script above to inspect whether the query plan changed.

## Large Dataset Query Experiments

KernelQ includes **`control_plane/sql/seed_large_jobs_dataset.sql`** to generate **thousands** of local synthetic jobs (mixed states, tenants, priorities). That helps inspect how **scheduler query plans** change as the `jobs` table grows. After seeding, rerun **`EXPLAIN`** / **`EXPLAIN ANALYZE`** with:

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/seed_large_jobs_dataset.sql

docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/explain_claim_schedulable_jobs.sql
```

## Database Test Isolation

Postgres-backed tests use **test-only job ID prefixes** (for example `test-repo-`, `test-tick-`, `test-api-`) and **clean up their own rows** before/after runs. This keeps test results stable even when local Postgres already contains large seed datasets or manual jobs.

## Kafka Infrastructure

KernelQ’s root **`docker-compose.yml`** now includes **Zookeeper** and **Kafka** for local development. Kafka will become the **durable coordination layer** between scheduler ticks (Python) and Go workers—buffering dispatch events and decoupling scheduling from execution. **Today this is infrastructure only:** start the broker with `docker compose up -d zookeeper kafka`; **publishing and consuming** come in a later milestone.

## Kafka Topics

KernelQ defines three topics: **`kernelq.jobs.dispatch`** (runnable work), **`kernelq.jobs.retry`** (failed jobs that can run again), and **`kernelq.jobs.dlq`** (dead-letter / poison messages). Create them locally with **`infra/kafka/create-topics.sh`** (see `docs/deploy.md`).

## Kafka Producer Skeleton

KernelQ has a Python **`KafkaJobProducer`** wrapper in **`kernelq/kafka_producer.py`**. It publishes **`DispatchEvent`** JSON to **`kernelq.jobs.dispatch`** (key = `job_id`). Tests in **`tests/test_kafka_producer.py`** inject a **fake producer**, so pytest does not need a running Kafka broker.

## Scheduler Kafka Publishing

**`SchedulerTickRunner`** can publish dispatch events to Kafka through the producer wrapper after **`claim_schedulable_jobs`**. Tests in **`tests/test_scheduler_tick.py`** use **`FakeJobProducer`**, so they do not require a broker. **Known gap:** if Postgres marks a job **`dispatched`** but publish fails, workers may never see it—a future **outbox** or **retryable dispatch** mechanism will fix that (see `docs/architecture.md`).

## Manual Scheduler-to-Kafka Smoke Test

**`scripts/run_scheduler_tick_once.py`** runs **one** scheduler tick with a **real `KafkaJobProducer`**: it claims **one** **`queued`** job from Postgres and publishes a **`DispatchEvent`** to **`kernelq.jobs.dispatch`**. You need a queued job already (API or SQL)—the script does not create one. Use the **Kafka CLI consumer** to read the message locally; **Go workers** will consume the topic later. Full steps: **`docs/deploy.md`** (Manual Scheduler-to-Kafka Smoke Test).

## Worker Result Event Contract

The Python control plane now includes a **`WorkerResultEvent`** model (`kernelq/result_event.py`). It **parses JSON messages** from **`kernelq.jobs.results`**—the topic where Go workers report execution outcomes.

Before any future **Postgres state update**, the model **validates** `event_type`, `job_id`, `status`, and `worker` (allowed statuses: `succeeded`, `retryable_failure`, `terminal_failure`). **Real Kafka result consumption** (subscribe, process, update job state) comes in a later step.

## Result Consumer Skeleton

KernelQ’s control plane now includes **`ResultConsumerRunner`** (`kernelq/result_consumer.py`). It takes a raw **`ResultMessage`** (Kafka key + JSON bytes), **parses and validates** it into a **`WorkerResultEvent`**, then **delegates** to a **`ResultHandler`**.

Tests use a **fake handler** so parsing and dispatch can be checked without a broker. **Real Kafka subscription** and **Postgres job-state updates** from result events come later.

## Result-to-State Handler

KernelQ’s control plane now includes **`ResultStateHandler`** (`kernelq/result_handler.py`). It maps validated **`WorkerResultEvent`** statuses to **`jobs.state`** in Postgres via **`JobRepository.update_job_state_from_worker_result`**.

- **`succeeded`** → **`SUCCEEDED`**
- **`retryable_failure`** → **`schedule_retry_from_worker_result`** (see **Retryable Result Handling**)
- **`terminal_failure`** → **`FAILED`** (for now)

## Retryable Result Handling

**`retryable_failure`** uses **`JobRepository.schedule_retry_from_worker_result`** (via **`ResultStateHandler`**):

- If **`retry_count < max_retries`**: increment **`retry_count`**, set **`retry_after`**, state → **`RETRY_SCHEDULED`**.
- If retries are **exhausted** (`retry_count >= max_retries`): state → **`DEAD_LETTERED`** (terminal; no auto-retry).

**`terminal_failure`** still maps to **`FAILED`** for now. Backoff tuning and **`kernelq.jobs.retry`** publish come later.

## Retry Requeue Scanner

**`RetryScanner`** only requeues jobs in **`RETRY_SCHEDULED`** whose **`retry_after <= now`** — it moves them to **`queued`**. It does **not** touch **`DEAD_LETTERED`** or other states. The **scheduler tick** then dispatches **`queued`** jobs again.

One-shot manual run:

```bash
PYTHONPATH=. python3 control_plane/scripts/run_retry_scanner_once.py
```

## Retry Requeue Smoke Test

**`scripts/smoke_retry_requeue.sh`** verifies **`retryable_failure` → `RETRY_SCHEDULED` → `QUEUED` → `DISPATCHED`** (no Go worker).

```bash
./control_plane/scripts/smoke_retry_requeue.sh
```

## Retry Exhaustion Smoke Test

**`scripts/smoke_retry_exhaustion.sh`** verifies that **exhausted retries move to `DEAD_LETTERED`**. It creates a **dispatched** job with **`retry_count = max_retries`**, injects a **`retryable_failure`** result through **`ResultStateHandler`**, and asserts the final state is **`dead_lettered`**. It then runs **`RetryScanner`** once to confirm **`DEAD_LETTERED`** jobs are **terminal** and **not requeued** (no Go worker).

```bash
./control_plane/scripts/smoke_retry_exhaustion.sh
```

## Dead-Lettered Job Inspection

The control plane can **list `DEAD_LETTERED` jobs** from Postgres via **`JobRepository.list_dead_lettered_jobs`** — this helps operators **inspect exhausted or permanently failed jobs** (`job_id`, retries, payload, timestamps).

```bash
PYTHONPATH=. python3 control_plane/scripts/list_dead_lettered_jobs.py
```

## Manual Dead-Letter Requeue

Operators can **manually requeue** a **`DEAD_LETTERED`** job after fixing root cause. The job moves back to **`QUEUED`**; **`retry_count`** is **preserved** for audit/history. This **does not dispatch** the job — the **scheduler tick** picks it up later.

```bash
PYTHONPATH=. python3 control_plane/scripts/requeue_dead_lettered_job.py <job_id>
```

## Job State Metrics Snapshot

**`JobRepository.count_jobs_by_state`** summarizes **current durable job states** in Postgres (how many rows per `queued`, `succeeded`, `dead_lettered`, etc.). **`GET /metrics/jobs`** returns the same snapshot as JSON (`job_state_counts`).

```bash
PYTHONPATH=. python3 control_plane/scripts/job_state_snapshot.py
```

## Job Duration Metrics

Jobs persist an optional **`dispatched_at`** timestamp (set on first **`queued` → `dispatched`**, never overwritten on retry re-dispatch). **`compute_job_duration_metrics`** derives averages from Postgres on completed jobs (`succeeded`, `failed`, `dead_lettered`):

- **Queue wait time** — `dispatched_at - created_at` (actual dispatch time; jobs without `dispatched_at` are skipped)
- **Queue wait percentiles** — **p50**, **p95**, **p99** from the same valid waits
- **Completion time** — `updated_at - created_at`

**Not histogram-based yet** — percentiles are computed from Postgres snapshots (nearest-rank), not Prometheus `_bucket` series.

CLI snapshot:

```bash
PYTHONPATH=. python3 control_plane/scripts/job_duration_snapshot.py
```

**Seed data:** **`scripts/seed_latency_metrics.py`** inserts 20+ succeeded jobs with realistic **`created_at` / `dispatched_at` / `updated_at`** (queue waits 1–10s) for local percentile testing and benchmarking.

```bash
PYTHONPATH=. python3 control_plane/scripts/seed_latency_metrics.py
```

HTTP: **`GET /metrics/durations`** returns averages plus **`p50_queue_wait_seconds`**, **`p95_queue_wait_seconds`**, **`p99_queue_wait_seconds`**. Same stats appear as **`kernelq_queue_wait_seconds{quantile=...}`** gauges on **`GET /metrics/prometheus`** (alongside job state counts).

**Smoke test:** **`scripts/smoke_queue_wait_metrics.sh`** — sleeps before dispatch, completes via **`ResultStateHandler`**, asserts **`queue_wait_seconds > 0`** (`dispatched_at - created_at`).

```bash
./control_plane/scripts/smoke_queue_wait_metrics.sh
```

## Load Job Generator

**`scripts/generate_load_jobs.py`** creates **`queued`** jobs directly in Postgres (via **`JobRepository`**, no HTTP API) for local benchmark preparation. Flags: **`--count`**, **`--prefix`**, **`--tenants`**, **`--max-priority`**. Jobs cycle tenants (`tenant-0`, …) and priorities (`0`..`max-priority`). Prints **`created_jobs`**, **`elapsed_seconds`**, **`jobs_per_second`**, plus a structured **`event=generate_load_jobs`** line.

```bash
PYTHONPATH=. python3 control_plane/scripts/generate_load_jobs.py --count 1000 --prefix bench --tenants 10
```

## Structured Script Logs

One-shot scripts print an extra **key=value summary line** at the end (via `kernelq/logging_utils.py` for Python scripts; bash helpers in smoke tests). Examples:

```
event=scheduler_tick selected_count=1 dispatched_count=1 published_count=1 errors_count=0 publish_errors_count=0
event=retry_scanner requeued_count=2 errors_count=0 requeued_job_ids=["job-a","job-b"]
event=result_consumer processed_message=true errors_count=0
event=job_state_snapshot total_jobs=5010 states_count=4
event=smoke_full_completion job_id=day52-full-123 final_state=succeeded success=true
```

**MVP smoke tests** emit `event=smoke_*` summary lines (`smoke_full_completion`, `smoke_retry_requeue`, `smoke_retry_exhaustion`, `smoke_queue_wait_metrics`) with `success=true|false` and state fields. Collect demo evidence after a run:

```bash
grep "event=smoke_" run.log
```

These lines are **grep-friendly** for local debugging and demos. This is **not a full logging stack** yet — no log levels, rotation, or centralized aggregation.

## Prometheus-Style Metrics

**`GET /metrics/prometheus`** exposes **durable Postgres job state counts** and **queue-wait percentile gauges** in Prometheus text exposition format (`kernelq_jobs_by_state`, `kernelq_queue_wait_seconds{quantile="0.50"|"0.95"|"0.99"}`). **Gauge quantiles from DB snapshots — not native Prometheus histograms yet.**

```bash
curl http://127.0.0.1:8000/metrics/prometheus
```

## Kafka Result Consumer Skeleton

KernelQ’s control plane now includes **`KafkaResultConsumer`** (`kernelq/kafka_result_consumer.py`) for **`kernelq.jobs.results`**. It **polls one result message** at a time and passes bytes through **`ResultConsumerRunner`** → **`ResultStateHandler`**, which can **update Postgres `jobs.state`**.

Manual try: **`control_plane/scripts/consume_result_once.py`**. A **long-running result consumer loop** (continuous poll, graceful shutdown) comes later.

## Full Completion Smoke Test

**`scripts/smoke_full_completion.sh`** is the **first MVP feedback-loop smoke test**. It verifies the full path from a **queued job** in Postgres to **`succeeded`** state: **scheduler tick** → **Kafka dispatch** → **Go worker** → **`kernelq.jobs.results`** → **Python result consumer** → **Postgres update**.

Requires Docker (Postgres + Kafka), Go, and Python. Run from the repository root:

```bash
./control_plane/scripts/smoke_full_completion.sh
```

## Responsibilities

The control plane is responsible for:

- **Scheduling**: Deciding when jobs should run based on schedules and priorities
- **State management**: Tracking where each job is in its lifecycle
- **API endpoints**: Providing REST APIs for creating and managing jobs
- **Orchestration**: Coordinating between components and handling failures
- **Retry logic**: Managing retries when jobs fail
- **Configuration**: Managing job definitions and system settings

Think of it like a manager: it makes plans, coordinates work, and handles problems. The workers (in Go) do the actual execution.

## Current Scheduling Policies

KernelQ’s control plane includes **three** in-Python schedulers:

- **FIFO (First-In, First-Out)** — our **baseline**: jobs leave the queue in arrival order. Simple and easy to compare against.
- **Priority scheduling** — jobs with **higher priority** run sooner, so urgent work can jump ahead of less important work.
- **Weighted round robin** — rotates dispatch turns across **tenants** using weights, improving **fairness** between customers and reducing **starvation** risk when the system is busy.

## Queue Control

KernelQ’s Python control plane now includes a **bounded queue**: a waiting line with a **fixed maximum size**.

**Admission control** is the idea that the system **chooses whether to accept** a new job. When the queue is **full**, new jobs are **rejected** (instead of piling up without limit), which avoids an **unbounded backlog** and gives callers a clear signal to **back off** or **retry later**.

This is an early **overload-protection** building block; a full production path would wire the same ideas into the API and metrics around Kafka dispatch.

## Backpressure Semantics

KernelQ’s Python control plane can return **explicit enqueue outcomes** instead of a plain yes/no: each attempt is **accepted**, **rejected because the queue is full**, or **rejected because the job is invalid** (for example a blank `job_id`).

That separation matters: overload is usually **retry with backoff**, while bad input needs a **client fix**. It is an early step toward **backpressure-aware APIs** and clearer **overload observability** (metrics and logs per reason).

## Current Combined Scheduler Design

KernelQ now has a **composed scheduler prototype** in Python. Instead of using one isolated policy, it combines a few decisions in order.

- **Admission first**: bounded queue capacity decides whether a new job is accepted.
- **Fairness across tenants**: weighted round robin picks which tenant gets the next turn.
- **Priority within a tenant**: higher-priority jobs run before lower-priority jobs.

This is our first combined scheduling pipeline, but it is still an **in-memory prototype** in the control plane.

## Current Measurement Layer

KernelQ now includes a small **scheduler metrics** module in Python (`scheduler_metrics.py`). It lets us tally **enqueue outcomes** (accepted vs full vs invalid), **dispatch counts** (totals, per tenant, per priority), and **peak queue depth** during simulations or tests.

A **simulation script** (`scripts/simulate_composed_scheduler.py`) runs a **repeatable** composed-scheduler experiment so we can inspect ordering and counters **before** Kafka and persistence are wired in.

## Current Scheduling Evaluation

KernelQ now measures **queue wait time** in the Python control-plane prototype, not just how many jobs were dispatched.

This lets us compare both **dispatch behavior** and **waiting behavior** by tenant and by priority.

That view helps us evaluate **fairness vs urgency tradeoffs** early, before Kafka and worker execution are fully wired in.

## Scheduler Comparison

KernelQ now includes a script (`scripts/compare_schedulers.py`) to compare multiple scheduling policies side by side.

All schedulers run on the **same fixed workload**, so differences in results come from scheduling policy, not from different inputs.

We compare **wait time**, **fairness across tenants**, and **dispatch behavior** to understand tradeoffs clearly.

This gives us a practical way to evaluate policy choices before Kafka dispatch and worker execution are fully integrated.

## Control Plane API

KernelQ now includes a **FastAPI-based REST control-plane API** for managing jobs and monitoring scheduler metrics.

The API includes endpoints to **enqueue jobs**, **query job states**, **cancel jobs**, **retry failed jobs**, and **retrieve scheduling metrics**.

It is designed so external clients and internal services can interact with the KernelQ scheduler through a clear HTTP interface.

## API Model Cleanup and State Safety

- **Enqueue** takes `job_id` from the **URL path** (`POST /jobs/{job_id}/enqueue`), not from the JSON body. The body carries fields like `tenant_id` and `priority` only.
- **Cancel** and **retry** call the shared state machine in `kernelq/job_state.py` (`can_transition`) before updating Postgres. Illegal moves return **409 Conflict**, not a silent bad state.
- One lifecycle definition for the API (and later the scheduler and workers) keeps state changes **predictable** as the system grows.

## Health Check and OpenAPI

The control plane exposes **`GET /health`** so load balancers and people can confirm the API process is up. For now it is a **shallow** check only (it does not probe dependencies).

FastAPI serves **interactive docs at `/docs`** and the **OpenAPI spec at `/openapi.json`** while the server is running.

Deeper checks for **Kafka, Postgres, and Redis** (and workers) will be added when those pieces are integrated.

## API Test Coverage

Automated API tests live in `tests/test_api.py` using FastAPI `TestClient`, so endpoint behavior is checked automatically (not only with manual curl requests).

These tests verify enqueue, query, cancel, retry, metrics, and error behavior.

This makes the API safer to change before we connect Postgres, Kafka, and Go workers.

## What job_state.py Models

The `job_state.py` file defines the job lifecycle state machine. It models:

- **All possible job states**: CREATED, QUEUED, DISPATCHED, RUNNING, SUCCEEDED, FAILED, RETRY_SCHEDULED, DEAD_LETTERED, CANCELED
- **Valid transitions**: Which states can move to which other states
- **Terminal states**: States that jobs cannot leave once reached
- **Transition validation**: Functions to check if a state change is allowed

This ensures jobs follow a predictable path and prevents them from getting stuck in undefined states.

## What's Coming Next

This control plane will grow to include:

- **REST API**: FastAPI endpoints for job management (create, read, update, delete jobs)
- **Scheduler**: Logic that decides when to move jobs from CREATED to QUEUED
- **Orchestration**: Coordinating retries, handling failures, managing dependencies
- **Database integration**: Storing job definitions and state in Postgres
- **Message broker integration**: Publishing jobs to the broker for workers to consume
- **Metrics and observability**: Tracking system health and performance

## Structure

- `kernelq/`: Core scheduling and job management logic
- `tests/`: Unit and integration tests
