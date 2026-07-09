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

The first migration, `control_plane/migrations/001_create_jobs.sql`, creates the **`jobs`** table. Later migrations add scheduler indexes and **`dispatched_at`**. The **FastAPI API** and **scheduler tick** both persist through **`JobRepository`**.

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

The **FastAPI API**, **scheduler tick**, **result handler**, and **benchmark scripts** all use this layer.

## Database-backed Scheduling Path

`JobRepository` supports **listing schedulable jobs** (`list_schedulable_jobs`), **atomic claiming** (`claim_schedulable_jobs`), and **marking dispatched** (`mark_job_dispatched`). **`SchedulerTickRunner`** uses **`claim_schedulable_jobs`** for durable, database-backed orchestration (priority, then age). Pass **`job_producer=None`** for Postgres-only benchmarks; pass **`KafkaJobProducer`** to publish after claim.

## Scheduler Tick Runner

KernelQ now has a **`SchedulerTickRunner`** (`kernelq/scheduler_tick.py`). Each **`run_once()`** tick calls **`claim_schedulable_jobs`** (up to `max_jobs_per_tick`), then optionally publishes **`DispatchEvent`** messages when a **`job_producer`** is passed in. **Day 108:** before publish, **`try_claim(dispatch_key(job_id, retry_count))`** — duplicates skip Kafka publish; **`duplicate_dispatches`** counter; **`event=duplicate_dispatch`**. It runs **synchronously** for now—no async yet.

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

KernelQ’s root **`docker-compose.yml`** includes **Zookeeper** and **Kafka** for local development. Kafka is the **coordination layer** between scheduler ticks (Python) and Go workers. Start locally with `docker compose up -d zookeeper kafka`, create topics with **`infra/kafka/create-topics.sh`**, then use **`run_scheduler_tick_once.py`** or the MVP smoke tests.

## Kafka Topics

KernelQ defines three topics: **`kernelq.jobs.dispatch`** (runnable work), **`kernelq.jobs.retry`** (failed jobs that can run again), and **`kernelq.jobs.dlq`** (dead-letter / poison messages). Create them locally with **`infra/kafka/create-topics.sh`** (see `docs/deploy.md`).

## Kafka Producer Skeleton

KernelQ has a Python **`KafkaJobProducer`** wrapper in **`kernelq/kafka_producer.py`**. It publishes **`DispatchEvent`** JSON to **`kernelq.jobs.dispatch`** (key = `job_id`). Tests in **`tests/test_kafka_producer.py`** inject a **fake producer**, so pytest does not need a running Kafka broker.

## Scheduler Kafka Publishing

**`SchedulerTickRunner`** can publish dispatch events to Kafka through the producer wrapper after **`claim_schedulable_jobs`**. Tests in **`tests/test_scheduler_tick.py`** use **`FakeJobProducer`**, so they do not require a broker. **Known gap:** if Postgres marks a job **`dispatched`** but publish fails, workers may never see it—a future **outbox** or **retryable dispatch** mechanism will fix that (see `docs/architecture.md`).

## Manual Scheduler-to-Kafka Smoke Test

**`scripts/run_scheduler_tick_once.py`** runs **one** scheduler tick with a **real `KafkaJobProducer`**: it claims **one** **`queued`** job from Postgres and publishes a **`DispatchEvent`** to **`kernelq.jobs.dispatch`**. You need a queued job already (API, SQL, or **`generate_load_jobs.py`**). **Go workers** consume the topic; see **`smoke_full_completion.sh`** and **`docs/deploy.md`**.

## Worker Result Event Contract

The Python control plane includes a **`WorkerResultEvent`** model (`kernelq/result_event.py`). It **parses and validates** JSON from **`kernelq.jobs.results`** (`event_type`, `job_id`, `status`, `worker`; statuses: `succeeded`, `retryable_failure`, `terminal_failure`). **`ResultStateHandler`** maps validated events to Postgres state updates.

## Result Consumer Skeleton

KernelQ’s control plane includes **`ResultConsumerRunner`** (`kernelq/result_consumer.py`). It takes a raw **`ResultMessage`** (Kafka key + JSON bytes), **parses and validates** it into a **`WorkerResultEvent`**, then **delegates** to a **`ResultHandler`**.

**Day 109:** worker execution idempotency **design** — **[worker-execution-idempotency.md](../docs/design/worker-execution-idempotency.md)** (`execution:<job_id>:<attempt>`; Kafka replay protection at worker boundary; not implemented). Dispatch + result dedupe integrated.

Tests use a **fake handler** so parsing can be checked without a broker. **`KafkaResultConsumer`** polls **`kernelq.jobs.results`** for one-shot manual runs; a **long-running consumer loop** is still future work.

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

## Scheduler Throughput Benchmark

**`scripts/benchmark_scheduler_throughput.py`** creates a local benchmark workload in Postgres, then dispatches it through repeated **`SchedulerTickRunner`** ticks (no Kafka). Use **`--trials`** to repeat runs with a **unique prefix per trial**; the summary reports **min/avg/max** **`jobs_dispatched_per_second`** (and elapsed time). Emits **`event=benchmark_scheduler_throughput`**. Measures **scheduler/Postgres dispatch throughput**, not worker execution.

```bash
PYTHONPATH=. python3 control_plane/scripts/benchmark_scheduler_throughput.py --count 1000 --batch-size 100 --tenants 10
PYTHONPATH=. python3 control_plane/scripts/benchmark_scheduler_throughput.py --count 100 --batch-size 20 --trials 3
```

**Benchmark reports:** **[day75-baseline.md](../docs/benchmarks/day75-baseline.md)** (first local baseline) and **[day77-scheduler-1000.md](../docs/benchmarks/day77-scheduler-1000.md)** (1000 jobs/trial, 3 trials, min/avg/max throughput). **Local development baselines only**—not production capacity claims.

## End-to-End Completion Benchmark

**Day 94+:** **`scripts/benchmark_end_to_end_completion.sh`** — full-system benchmark: **`queued` → `dispatched` → worker result → `succeeded`**. **`TRIALS`** (default **`1`**) with **min/avg/max** when **`TRIALS>1`**. **Complements** scheduler and worker benchmarks. **Local dev only — not production capacity.** Report: **[day94-end-to-end-completion.md](../docs/benchmarks/day94-end-to-end-completion.md)**.

```bash
./control_plane/scripts/benchmark_end_to_end_completion.sh
TRIALS=2 COUNT=5 ./control_plane/scripts/benchmark_end_to_end_completion.sh
```

## Idempotency Store

**Day 98:** **`kernelq/idempotency_store.py`** — **`IdempotencyStore`**, test-only **`InMemoryIdempotencyStore`**, and **`RedisIdempotencyStore`** (duck-typed Redis client, **`SET NX EX`**). Unit tests use a **fake client**; optional live check: **`scripts/smoke_redis_idempotency.py`** (redis-cli via Docker).

**Day 99:** **`kernelq/idempotency_keys.py`** — canonical key builders: **`worker_result_key`**, **`dispatch_key`**, **`execution_key`**, **`event_key`**.

**Day 100–104:** **`ResultConsumerRunner`** — **`worker_result_key`** dedupe; **`processed_messages`**, **`duplicate_messages`**; **`event=duplicate_worker_result`**. **Day 102:** env backend. **Day 103:** Redis consumer smoke. Design: **[redis-idempotency-deduplication.md](../docs/design/redis-idempotency-deduplication.md)**.

**Day 101:** **`scripts/smoke_result_idempotency_redis.py`** — live **`worker_result_key`** + Redis **`SET NX EX`** smoke (redis-cli via Docker).

**Day 102:** **`kernelq/idempotency_config.py`** — **`build_idempotency_store_from_env()`**. Env: **`KERNELQ_IDEMPOTENCY_BACKEND`** (`memory` default, `redis`); **`KERNELQ_REDIS_HOST`**, **`KERNELQ_REDIS_PORT`**, **`KERNELQ_REDIS_NAMESPACE`**. Redis backend uses **redis-cli subprocess** (no Python Redis package). Redis down → **`RuntimeError`** (fail clear).

**Day 105–106:** **`format_result_consumer_metrics`**; **`GET /metrics/prometheus`** includes dedupe counters (zeros until shared stats). Future: persistent counters, Grafana, CloudWatch.

```bash
python3 -m pytest control_plane/tests/test_idempotency_config.py control_plane/tests/test_idempotency_store.py control_plane/tests/test_redis_idempotency_store.py control_plane/tests/test_idempotency_keys.py control_plane/tests/test_result_consumer.py
PYTHONPATH=. python3 control_plane/scripts/consume_result_once.py
PYTHONPATH=. python3 control_plane/scripts/smoke_result_idempotency_redis.py
PYTHONPATH=. python3 control_plane/scripts/smoke_result_consumer_redis_idempotency.py
```

## Structured Script Logs

One-shot scripts print an extra **key=value summary line** at the end (via `kernelq/logging_utils.py` for Python scripts; bash helpers in smoke tests). Examples:

```
event=scheduler_tick selected_count=1 dispatched_count=1 published_count=1 duplicate_dispatches=0 errors_count=0 publish_errors_count=0
event=retry_scanner requeued_count=2 errors_count=0 requeued_job_ids=["job-a","job-b"]
event=result_consumer processed_message=true errors_count=0
event=result_consumer_summary duplicate_messages=0 idempotency_backend=memory processed_messages=1
event=duplicate_worker_result job_id=job-abc attempt=0
event=job_state_snapshot total_jobs=5010 states_count=4
event=generate_load_jobs created_jobs=1000 elapsed_seconds=1.2 jobs_per_second=833.3 tenants=10
event=benchmark_scheduler_throughput dispatched_jobs=1000 generated_jobs=1000 jobs_dispatched_per_second=4200.0 tick_count=20
event=benchmark_end_to_end_completion generated_jobs=10 dispatched_jobs=10 succeeded_jobs=10 jobs_completed_per_second=0.5 worker_count=4 queue_capacity=100 job_prefix=e2e-bench-123
event=smoke_full_completion job_id=day52-full-123 final_state=succeeded success=true
event=smoke_redis_idempotency success=true
event=smoke_result_idempotency_redis success=true
event=smoke_result_consumer_redis_idempotency success=true
```

**MVP smoke tests** emit `event=smoke_*` summary lines (`smoke_full_completion`, `smoke_retry_requeue`, `smoke_retry_exhaustion`, `smoke_queue_wait_metrics`) with `success=true|false` and state fields. Collect demo evidence after a run:

```bash
grep "event=smoke_" run.log
```

These lines are **grep-friendly** for local debugging and demos. This is **not a full logging stack** yet — no log levels, rotation, or centralized aggregation.

## Prometheus-Style Metrics

**`GET /metrics/prometheus`** exposes **Postgres job state counts**, **queue-wait percentile gauges**, and **result-consumer dedupe counters** (`kernelq_jobs_by_state`, `kernelq_queue_wait_seconds{quantile=...}`, `kernelq_result_consumer_processed_messages`, `kernelq_result_consumer_duplicate_messages`). Counters default to **0** until shared/persisted result-consumer stats are wired. **Gauge quantiles from DB snapshots — not native Prometheus histograms yet.**

```bash
curl http://127.0.0.1:8000/metrics/prometheus
```

## Kafka Result Consumer Skeleton

KernelQ’s control plane now includes **`KafkaResultConsumer`** (`kernelq/kafka_result_consumer.py`) for **`kernelq.jobs.results`**. It **polls one result message** at a time and passes bytes through **`ResultConsumerRunner`** → **`ResultStateHandler`**, which can **update Postgres `jobs.state`**.

Manual try: **`control_plane/scripts/consume_result_once.py`**. A **long-running result consumer loop** (continuous poll, graceful shutdown) comes later.

## Go Worker Pool

The **Go worker** (`worker/`, see **`worker/README.md`**) uses a **configurable worker pool** for concurrent execution. **Default: 4 workers.** The **Kafka consumer** reads dispatch messages from **`kernelq.jobs.dispatch`**; **pool workers** execute jobs in parallel (`Executor` + result publish). Configure via **`KafkaConsumer.WorkerCount`** in **`cmd/consumer`**.

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

A **simulation script** (`scripts/simulate_composed_scheduler.py`) runs a **repeatable** composed-scheduler experiment for in-memory policy comparison. **Durable scheduling** uses **`SchedulerTickRunner`** against Postgres; see **Scheduler Throughput Benchmark** for dispatch-rate measurement.

## Current Scheduling Evaluation

KernelQ measures **queue wait time** in simulations and in **production Postgres metrics** (`job_duration_snapshot.py`, **`GET /metrics/durations`**, Prometheus gauges).

This lets us compare **dispatch behavior** and **waiting behavior** by tenant and priority—in prototypes and on real job rows with **`dispatched_at`**.

## Scheduler Comparison

KernelQ now includes a script (`scripts/compare_schedulers.py`) to compare multiple scheduling policies side by side.

All schedulers run on the **same fixed workload**, so differences in results come from scheduling policy, not from different inputs.

We compare **wait time**, **fairness across tenants**, and **dispatch behavior** to understand tradeoffs. **Durable dispatch** is exercised via **`benchmark_scheduler_throughput.py`** and smoke tests end-to-end with Go workers.

## Control Plane API

KernelQ now includes a **FastAPI-based REST control-plane API** for managing jobs and monitoring scheduler metrics.

The API includes endpoints to **enqueue jobs**, **query job states**, **cancel jobs**, **retry failed jobs**, and **retrieve scheduling metrics**.

It is designed so external clients and internal services can interact with the KernelQ scheduler through a clear HTTP interface.

## API Model Cleanup and State Safety

- **Enqueue** takes `job_id` from the **URL path** (`POST /jobs/{job_id}/enqueue`), not from the JSON body. The body carries fields like `tenant_id` and `priority` only.
- **Cancel** and **retry** call the shared state machine in `kernelq/job_state.py` (`can_transition`) before updating Postgres. Illegal moves return **409 Conflict**, not a silent bad state.
- One lifecycle definition for the API (and later the scheduler and workers) keeps state changes **predictable** as the system grows.

## Health Check and OpenAPI

The control plane exposes **`GET /health`** so load balancers and people can confirm the API process is up. For now it is a **shallow** check only (it does not probe Postgres or Kafka).

FastAPI serves **interactive docs at `/docs`** and the **OpenAPI spec at `/openapi.json`** while the server is running.

**Future work:** dependency-aware health (Postgres, Kafka, worker lag) and readiness endpoints.

## API Test Coverage

Automated API tests live in `tests/test_api.py` using FastAPI `TestClient`, so endpoint behavior is checked automatically (not only with manual curl requests).

These tests verify enqueue, query, cancel, retry, metrics, and error behavior against the **Postgres-backed API** (with test isolation via job-id prefixes).

## What job_state.py Models

The `job_state.py` file defines the job lifecycle state machine. It models:

- **All possible job states**: CREATED, QUEUED, DISPATCHED, RUNNING, SUCCEEDED, FAILED, RETRY_SCHEDULED, DEAD_LETTERED, CANCELED
- **Valid transitions**: Which states can move to which other states
- **Terminal states**: States that jobs cannot leave once reached
- **Transition validation**: Functions to check if a state change is allowed

This ensures jobs follow a predictable path and prevents them from getting stuck in undefined states.

## What's Coming Next

Planned improvements beyond the current MVP checkpoint (see **[docs/mvp.md](../docs/mvp.md)**):

- **Outbox / retryable dispatch** — reconcile Postgres **`dispatched`** rows when Kafka publish fails
- **Long-running result consumer** — continuous poll loop with graceful shutdown
- **Worker throughput benchmarks** — pool size and backpressure experiments
- **Native Prometheus histograms** — replace snapshot-derived quantile gauges
- **Dependency-aware health checks** — Postgres, Kafka, and worker readiness
- **Cloud deployment** — beyond local Docker Compose

## Structure

- `kernelq/`: Core scheduling and job management logic
- `tests/`: Unit and integration tests
