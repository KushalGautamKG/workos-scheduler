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

KernelQ now has a **`SchedulerTickRunner`** (`kernelq/scheduler_tick.py`). Each **`run_once()`** tick calls **`claim_schedulable_jobs`** (up to `max_jobs_per_tick`). It runs **synchronously** for now—no async and no **Kafka publishing** yet.

## Atomic Job Claiming

Scheduler ticks claim work through **`JobRepository.claim_schedulable_jobs()`**: it **selects `queued` jobs and marks them `dispatched` in one Postgres transaction**, which **reduces duplicate dispatch risk** when multiple schedulers run. The SQL uses row locking with **`FOR UPDATE SKIP LOCKED`** so instances skip rows another scheduler is already claiming. **Kafka publishing** still comes later.

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
