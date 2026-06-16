# Architecture

## High-Level Overview

This system is a distributed work coordination and scheduling platform. It behaves like an operating system for backend jobs. The system is split into two planes: a control plane that makes decisions and a worker plane that executes tasks.

The control plane handles scheduling, state management, and coordination. The worker plane handles high-throughput task execution. They communicate through **Kafka**.

## Control Plane (Python)

The control plane is responsible for:

- **Scheduling decisions**: Deciding when jobs should run based on schedules, priorities, and resource availability
- **Job state management**: Tracking job lifecycle states (pending, queued, running, completed, failed)
- **API endpoints**: Exposing REST APIs for creating, updating, and querying jobs
- **Orchestration**: Coordinating between components, managing retries, and handling failures
- **Configuration**: Managing job definitions, schedules, and system settings
- **Observability**: Collecting metrics and logs from the control plane operations

The control plane prioritizes flexibility and rapid development over raw performance. It handles lower-frequency operations like API requests and scheduling logic.

## Worker Plane (Go)

The worker plane is responsible for:

- **Task execution**: Running the actual job code when it's time
- **Broker consumption**: Pulling jobs from Kafka efficiently
- **Concurrency management**: Handling thousands of concurrent task executions
- **Resource isolation**: Enforcing limits per job, tenant, or resource type
- **Execution metrics**: Reporting task completion, latency, and errors back to the control plane
- **Failure handling**: Implementing retries, timeouts, and circuit breakers at execution time

The worker plane prioritizes throughput, low latency, and resource efficiency. It must handle high-frequency operations reliably.

## Main Components

- **API Gateway**: Entry point for external requests (REST API)
- **Scheduler**: Decides when jobs should run (part of control plane)
- **Job State Machine**: Manages job lifecycle transitions (pending → queued → running → completed/failed)
- **Kafka**: Durable messaging backbone used to transport runnable jobs from the Python control plane to Go workers, support consumer-group-based scaling, and enable retry / replay workflows.
- **Postgres**: Persistent storage for job definitions, schedules, and state
- **Redis**: Caching layer
- **Workers**: Go processes that consume from Kafka and execute tasks

## FIFO Scheduling Policy

FIFO means **First-In, First-Out**: the first job that enters the queue is the first job that should be chosen to run. If jobs arrive in the order A, B, C, then a FIFO scheduler will pick A first, then B, then C.

FIFO is the simplest scheduling baseline because it is:

- **Easy to reason about**: order is predictable and matches arrival order
- **Easy to implement**: minimal logic beyond a queue
- **A good reference point**: it gives a clear “default” behavior to compare against more advanced policies

In KernelQ, FIFO fits as **Python control plane scheduler logic**: the control plane decides which queued job should be dispatched next, and FIFO is the most straightforward way to produce that ordering.

FIFO has important limitations:

- **No notion of priority**: it cannot intentionally run more important jobs first
- **No fairness across tenants**: a busy tenant can dominate the queue and crowd out others
- **Can delay urgent work behind older jobs**: urgent jobs may wait a long time if earlier jobs are already ahead in line

## Priority Scheduling Policy

**Priority scheduling** means the scheduler chooses what to run next based on **importance or urgency**, not only on **arrival order**. Each job carries a priority (for example high / normal / low, or a numeric rank). When picking the next runnable job, the scheduler prefers **higher-priority** work over lower-priority work.

**How it differs from FIFO:** **FIFO** only asks “who got here first?” **Priority scheduling** also asks “who matters most?” So a **newer** high-priority job can be ordered **ahead of** an **older** low-priority job—something pure FIFO will never do.

**Where it fits in KernelQ:** Priority rules live in the **Python control plane scheduler logic**: the control plane decides **which queued job to dispatch next** (and in what order). Workers in Go **execute**; they do not own the policy that decides global priority among waiting jobs.

**A major limitation:** naive priority scheduling can cause **starvation**—low-priority jobs may wait a very long time (or never run) if higher-priority work keeps arriving. Real systems often add **fairness** mechanisms (e.g., aging, caps, or tenant quotas) so low-priority work still makes progress.

## Starvation and Fairness

**Starvation** (in scheduling) means some work waits **far too long** or **never gets a turn**, even though the system is still busy processing other jobs. The starved jobs are stuck behind a policy or load pattern that never favors them.

**Why naive priority can starve low-priority jobs:** if the scheduler *always* prefers higher priority, and higher-priority jobs **keep arriving**, lower-priority jobs may **never reach the front of the line**. There is no rule that guarantees them *any* progress—only that *more important* work goes first.

**Fairness** means the scheduler tries to give **each tenant or queue a fair share of progress** over time—not letting one customer or job class **monopolize** the system forever, even when priorities exist. Fairness is often implemented with **limits**, **quotas**, **aging** (boosting jobs that wait too long), or **round-robin** style turns across tenants.

In KernelQ, starvation and fairness concerns inform **how we design** the Python control plane scheduler: not just *who is most important*, but *who still gets to run* when the system is overloaded.

## Weighted Round Robin Scheduling Policy

**Weighted round robin (WRR)** is a way to serve **multiple queues** (often one per **tenant** or **job class**) in **rotating turns**. Each queue gets repeated chances to dispatch a job. **Weights** set how strong each queue’s share is—for example, a weight of `2` might mean “roughly twice as many turns” as a weight of `1` in each full cycle.

**How WRR helps compared to naive priority:** naive priority can **starve** whole categories of work. WRR adds **structure**: even busy tenants take turns according to their weight, so quieter tenants are not **permanently crowded out** by a flood of high-priority work elsewhere.

**A limitation:** WRR is often **fairer across tenants**, but it **does not by itself solve every latency or priority problem**. You can still have urgent jobs delayed if the policy does not combine WRR with **priority**, **SLO-aware rules**, or **per-tenant caps**. Choosing weights can also be subtle: “fair” sharing is not the same as “optimal” for every workload.

**Where it fits in KernelQ:** weighted round robin is **Python control plane scheduler logic**—the layer that decides **which tenant’s queue** (or which class of job) gets the next dispatch opportunity. Go workers **run** jobs; they do not decide global rotation and weights across tenants.

## Bounded Queues and Admission Control

A **bounded queue** is a waiting line with a **maximum capacity**. When it is full, the system **stops accepting more items** in that queue until space opens up (for example, after jobs are dispatched or complete). That cap is intentional: it keeps memory and backlog under control.

**Admission control** is the policy that answers: **“Should we accept this new job right now?”** It runs *before* work enters deep queues or downstream systems. If the system is already saturated, admission control **rejects**, **rate-limits**, or **defers** new submissions instead of pretending everything can be handled immediately.

**Why unbounded queues are dangerous in a distributed system:** if queues can grow without limit, overload in one place spreads as **unbounded memory use**, **long unpredictable delays**, and **cascading failures** (every component keeps accepting work it cannot finish). The failure mode becomes “the whole cluster falls over” rather than “the API says *not now* and clients back off.”

**When KernelQ is full:** the **Python control plane** should **reject new work** (or apply an explicit overflow policy), not silently accept an **unlimited backlog**. Clients then know to **retry later**, **reduce load**, or **route elsewhere**. That protects Postgres, Kafka, and workers from being drowned by work the system cannot make progress on.

**Where this fits in KernelQ:** bounded queues and admission control belong in the **Python control plane**, **before** jobs are **dispatched to Kafka**—at the API and scheduling layers where jobs are first admitted and queued. Once work is safely bounded and intentional at the edge, downstream components can rely on predictable load.

## Backpressure and Rejection Semantics

**Backpressure** means the system **signals “slow down” upstream** when a downstream part is overloaded. Instead of every layer silently accepting unlimited work, pressure travels backward: clients may **retry later**, **send less traffic**, or **wait**, so the whole pipeline stays stable.

**How bounded queues and admission control relate:** a **bounded queue** creates a natural **limit**; **admission control** is the **gate** that enforces it. When the queue is full, new submissions fail fast at the edge—that is a form of **backpressure** because upstream learns the system cannot absorb more work *right now*.

**Why explicit rejection reasons beat a plain True/False:** a single boolean hides *why* something was refused. Operators and clients need to distinguish **overload** from **bad input** from **auth failures**, and so on. Typed outcomes (or error codes / structured errors) make behavior **explainable**, **testable**, and **observable**.

**KernelQ-style outcomes to keep separate:**

- **Accepted** — the job passed validation and was admitted under current capacity; it can progress through the lifecycle.
- **Rejected: queue full** — the request may be fine, but the system is **temporarily saturated**; the right client behavior is usually **retry with backoff**, and operators watch **saturation metrics**.
- **Rejected: invalid request/job** — the payload or job definition breaks rules (schema, constraints, impossible schedule); **retrying the same request will not help** until the client **fixes the input**.

**Where this fits in KernelQ:** rejection semantics are enforced in the **Python control plane**—API validation, admission checks, and queueing—**before** work is **dispatched to Kafka**. That is where KernelQ can return **clear, distinct outcomes** to callers and attach **metrics and logs** per reason.

## Combined Scheduling Pipeline

Real schedulers rarely rely on one isolated policy. In KernelQ, we now combine multiple scheduling decisions into one pipeline so the system can handle overload, fairness, and urgency together.

The combined pipeline is:

1. **Validate and admit jobs** using bounded queue rules. A job is accepted only if it is valid and there is queue capacity; otherwise it is rejected clearly (for example, invalid job or queue full).
2. **Choose the next tenant** using **weighted round robin** fairness so one noisy tenant cannot dominate dispatch forever.
3. **Choose the next job within that tenant** using **priority** so more urgent work can run first inside that tenant's queue.
4. **Break ties by `created_at`** so jobs with equal priority are served in predictable oldest-first order.

This is more realistic than pure FIFO or pure priority alone. Pure FIFO is simple but ignores urgency and cross-tenant fairness. Pure priority helps urgency but can starve some work. The combined pipeline is a practical middle ground that reflects how production schedulers are usually designed.

The **scheduling policies** above are still exercised mainly as **in-memory Python prototypes** for simulation and metrics. The **HTTP job API** now persists rows in Postgres via `JobRepository`; Kafka dispatch and Go workers are the next integration step.

## PostgreSQL as Durable Job State Store

KernelQ’s scheduling logic and API prototypes often keep jobs **in memory** for speed and simplicity during development. That is fine for experiments, but **memory is not durable**: restart the process, lose power, or deploy a new instance and **unpersisted job rows disappear**.

**PostgreSQL becomes the durable source of truth for jobs** in KernelQ. Once a job is written to Postgres, the control plane, workers, and operators can agree on **what exists**, **what state it is in**, and **what happened last**—even after crashes, rolling deploys, or scaling out.

Jobs need **durable state** so the system can **recover**: retries don’t double-book mystery work, dashboards stay truthful, and dispatch decisions can resume from a consistent checkpoint instead of guessing.

The **`jobs` table** holds what every participant needs to coordinate:

- **tenant_id** and **priority** for fairness and scheduling policy inputs  
- **state** for the lifecycle (queued, running, failed, and so on)  
- **retry_count** and **max_retries** for explicit retry policy  
- **payload** (typically JSON) for client-provided or enriched job inputs  
- **created_at** / **updated_at** for auditing and ordering  

**Indexes** make common queries fast at scale (interview point: *indexes match your read patterns*):

- **state** — “How many jobs are queued?” / “List everything stuck in *failed*”  
- **tenant_id** — per-customer views, quotas, and support lookups  
- **priority** — “Run the most important work first” within a policy  
- **(state, tenant_id)** — “What is this customer still waiting on in *queued*?” (very common operationally)  
- **(state, priority)** — “Among jobs ready to run, which should we pick next by priority?”

Together, durable rows + the right indexes turn KernelQ from a demo into something you can run in production and reason about when things go wrong.

## Repository Layer

The Python control plane now includes a **repository layer** for job persistence (for example `JobRepository` alongside a small DB connection helper). Routes and scheduling code call **repository methods** instead of embedding long SQL strings everywhere.

The repository **hides SQL details** behind a small, testable surface: create, read, update, and delete jobs by intent. That keeps HTTP handlers and policy code readable and makes it easier to swap or mock storage in tests.

**Postgres remains the durable source of truth** for job state: the repository is how the application reads and writes that truth, not a second copy of business rules.

Queries use **parameterized SQL** (bound parameters, not Python string formatting) so values are never concatenated into query text—a basic safety and correctness practice you should mention in interviews when discussing SQL injection and maintainable data access.

## Database-backed Scheduling Query Path

KernelQ’s **in-memory schedulers** (FIFO, priority, composed) are great for learning and simulation, but they disappear when the process restarts. The control plane now has **repository methods** that ask **Postgres** which jobs are ready to run next— the first concrete step from “policy in Python memory” to **durable, database-backed orchestration**.

**What “schedulable” means (for now):** a job row whose **`state` is `queued`**. That is the waiting line the dispatcher is allowed to drain. Jobs in other states—`created` (not queued yet), `dispatched` (already handed off), `running`, terminal states, or `retry_scheduled` (waiting for backoff)—are **not** returned by the schedulable query.

**How the next jobs are ordered:** the repository selects schedulable rows with:

```sql
WHERE state = 'queued'
ORDER BY priority DESC, created_at ASC
LIMIT <n>
```

- **`priority DESC`** — more urgent work first (larger priority number runs sooner).
- **`created_at ASC`** — when priorities tie, **older jobs first** (FIFO among equals), so ordering stays predictable and fair.

**Methods on `JobRepository` (today):**

- **`list_schedulable_jobs(limit=10)`** — runs the query above and returns a list of `JobRecord` objects. A future dispatch loop will call this to decide **what to pick next**.
- **`mark_job_dispatched(job_id)`** — after a job is selected, moves it from **`queued` → `dispatched`** (only if it is still `queued`). That **claims** the row so another scheduler pass does not pick the same job again, and it matches the lifecycle: *waiting in Postgres* → *handed off toward workers* → *running*.

**Why mark `dispatched`?** Without that step, a job could stay `queued` in the database while already being sent downstream—risking **double dispatch** and making operations unclear (“is it waiting or in flight?”). `dispatched` is the bridge state before Kafka and Go workers are fully wired in.

**What is not connected yet:** this path **does not publish to Kafka**. It answers “which rows should run next?” and “mark this one as handed off” in Postgres only. **Kafka dispatch**—publish runnable jobs, consumer groups, worker pickup—is the **next** milestone. The in-memory prototypes taught **policy**; the repository query path teaches **durability and safe handoff**.

**Interview sound bite:** *“Schedulable means `queued` in Postgres, ordered by priority then age; `mark_job_dispatched` claims the row. In-memory schedulers taught ordering rules; the repository makes those rules survive restarts—Kafka comes later.”*

## Scheduler Tick Loop

KernelQ now has a **scheduler tick runner** in the **Python control plane** (`control_plane/kernelq/scheduler_tick.py`). A **tick** is one pass of the dispatch loop: claim waiting jobs in Postgres, optionally publish dispatch events to Kafka, return a summary. Call it from a loop, cron, or background task—**synchronous** and **simple** today.

**What happens in one tick (`SchedulerTickRunner.run_once()`):**

1. **Atomically claim jobs** — `claim_schedulable_jobs(limit=max_jobs_per_tick)` selects up to *N* **`queued`** rows (ordered by **`priority DESC`**, then **`created_at ASC`**) and marks them **`dispatched`** in **one transaction** (see **Atomic Job Claiming** below).
2. **Optionally publish to Kafka** — when a **`job_producer`** is injected, build a **`DispatchEvent`** per claimed row and call **`publish_dispatch_event`** on **`kernelq.jobs.dispatch`** (see **Scheduler to Kafka Dispatch Flow**).
3. **Cap batch size** — `max_jobs_per_tick` limits how many rows one pass can claim.
4. **Return a summary** — **`SchedulerTickResult`** reports claimed ids, **`published_count`**, repository **`errors`**, and per-message **`publish_errors`**.

**What `dispatched` means:** the scheduler **claimed** the job in Postgres (`queued` → `dispatched`). With a producer wired in, the tick also **attempts** Kafka publish immediately after claim. Workers still consume from the topic and update Postgres toward **`running`**.

**Where it lives:** the tick runner belongs in the **Python control plane** alongside `JobRepository` and the lifecycle state machine. **Go workers** execute jobs; they do not run this loop.

**Interview sound bite:** *“Each tick claims in Postgres, then publishes `DispatchEvent` JSON to `kernelq.jobs.dispatch` when a producer is configured.”*

## Atomic Job Claiming

The scheduler tick now uses **`JobRepository.claim_schedulable_jobs()`** instead of “list jobs, then mark each one dispatched” as separate steps. That method **selects `queued` jobs and updates them to `dispatched` in a single Postgres transaction**, which **reduces duplicate dispatch risk** when more than one scheduler runs at once.

**The duplicate dispatch problem (plain English):** two schedulers can both think they should run the same job if they **read** “job A is `queued`” before either **writes** “job A is `dispatched`.” Both might publish to Kafka or hand work to workers twice. KernelQ avoids that by making **pick + claim** one atomic database operation.

**Race condition (plain English):** a **race** is when the outcome depends on **timing**—who finishes first. Scheduler A and Scheduler B racing on the same row is a classic race: without locking or atomic updates, both can win the read step and both try to dispatch.

**What the SQL pattern does:** `claim_schedulable_jobs` uses a subquery like:

```sql
SELECT job_id FROM jobs
WHERE state = 'queued'
ORDER BY priority DESC, created_at ASC
LIMIT <n>
FOR UPDATE SKIP LOCKED
```

then updates those rows to **`dispatched`**.

- **`FOR UPDATE`** — “I am about to change these rows; lock them for this transaction.”
- **`SKIP LOCKED`** — if another scheduler already locked a row, **do not wait**—skip it and take the next available `queued` rows.

So multiple instances **work in parallel** without blocking each other on the same jobs, and without picking the same locked rows.

**Why one transaction matters:** the **select-with-lock** and the **update to `dispatched`** commit together. Other sessions do not see a row stuck as `queued` after you have already claimed it inside that transaction.

**Older methods still exist:** `list_schedulable_jobs` and `mark_job_dispatched` remain useful for tests and teaching, but **`SchedulerTickRunner` uses `claim_schedulable_jobs`** for production-style ticks.

**Preparing for multiple scheduler instances:** today you can run one tick loop in development; tomorrow you might run **several control-plane processes** (or pods) each calling `run_once()` on a timer. Atomic claiming is how KernelQ **shares the queue safely** across those instances before Kafka publish is added.

**Interview sound bite:** *“We claim in one transaction with `FOR UPDATE SKIP LOCKED` so two schedulers don’t dispatch the same job—locked rows are skipped, not double-picked.”*

## Scheduler Query Indexing

The scheduler **repeatedly** asks Postgres for the next batch of waiting work. That query has a stable shape: filter by **`state`** (only **`queued`** rows), then order by **`priority`** (urgent first) and **`created_at`** (older jobs first when priority ties)—the same policy as the in-memory priority scheduler, but executed on every tick.

KernelQ adds a Postgres index to support that pattern (migration **`002_add_scheduler_claim_index.sql`**):

```sql
idx_jobs_state_priority_created_at ON jobs (state, priority DESC, created_at ASC)
```

This connects **scheduling policy** to **database physical design**: the index column list mirrors `WHERE state = 'queued'` and `ORDER BY priority DESC, created_at ASC` used by `list_schedulable_jobs` and `claim_schedulable_jobs`. As the `jobs` table grows, the planner can find schedulable rows in dispatch order instead of scanning unrelated history on every tick. Use **`EXPLAIN`** before and after applying the migration to confirm the plan (see `docs/perf.md`).

**Interview sound bite:** *“Our claim query filters on state and sorts by priority then age—we added a matching composite index so policy and Postgres storage stay aligned.”*

## Scheduler Query Scaling

Scheduler queries **may behave differently** as the **`jobs`** table grows. On a handful of rows, Postgres often prefers a **sequential scan**; with thousands of rows and only a slice in **`queued`**, the same query may benefit from **`idx_jobs_state_priority_created_at`**—but the only way to know is to inspect the plan on realistic data.

KernelQ includes **synthetic seed data** for local experiments: **`control_plane/sql/seed_large_jobs_dataset.sql`** inserts about **5000** jobs with mixed states and tenants. That is **not production traffic**—it helps you **validate whether scheduler indexes become useful at larger scales** before load testing. Run seed, then **`explain_claim_schedulable_jobs.sql`**, and compare **`EXPLAIN`** output (see **`docs/perf.md`**, **Large Dataset Query Plan Experiment**).

## API to Repository Flow

The control-plane **FastAPI routes** (`control_plane/api.py`) now talk to jobs through **`JobRepository`**, not by embedding SQL in every handler. That is a simple **layered design** you can draw on a whiteboard in an interview.

**Request path (today):**

1. **Client** calls a route such as `POST /jobs/{job_id}/enqueue` or `GET /jobs/{job_id}`.
2. **FastAPI handler** validates the HTTP request (body shape, `job_id` from the URL path, state-transition rules via `job_state.py`).
3. **`JobRepository`** runs the database work: `create_job()`, `get_job()`, `update_job_state()`, and similar methods.
4. **PostgreSQL** stores the result in the **`jobs` table**—the **durable source of truth** for job identity, state, retries, and payload.

**Why separate API from SQL:**

- **API logic** stays focused on HTTP status codes, validation, and orchestration (“what should happen for this request?”).
- **`JobRepository` owns SQL access** to the `jobs` table—INSERT/SELECT/UPDATE with parameterized queries live in one place.
- **Postgres** remains the system of record; the repository is how Python reads and writes that record, not a second copy of state in memory.

**Interview sound bite:** *“Routes don’t know table column lists; the repository does. Postgres is truth; the API is the front door.”*

**Not connected yet:** This flow **does not** publish to **Kafka** or assign work to **Go workers**. Enqueue/cancel/retry today update **durable rows** only. Worker consumption, dispatch, and execution metrics will plug in later without rewriting every SQL string in the API.

## State Transition Enforcement in the API

The HTTP API **does not invent its own state rules**. Handlers for cancel and retry do not hard-code one-off `if state == ...` trees that could drift from the rest of the system.

Instead, routes use the **shared job lifecycle state machine** in `control_plane/kernelq/job_state.py`:

- **`JobState`** — all legal states (stored in Postgres as lowercase strings such as `queued`, `failed`, `canceled`)
- **`can_transition(from, to)`** — whether a move is allowed
- **`explain_transition(from, to)`** — human-readable reason when a move is rejected

**Why this matters:** The same module can drive **API behavior today**, **scheduler dispatch decisions tomorrow**, and **future worker status updates** without three different definitions of “can this job be retried?” One policy, many callers—easier to test, document, and defend in an interview.

**Conflict, not silent corruption:** If a client requests an illegal transition (for example retry while `queued`, or cancel after `succeeded`), the API returns **409 Conflict** with `detail` from `explain_transition()`. It does **not** write a bad state to Postgres. That fail-fast behavior protects operators and downstream automation from believing a job is in a state the lifecycle never allows.

**Interview sound bite:** *“Postgres stores current state; `job_state.py` defines allowed moves; the API enforces moves before UPDATE—illegal requests get 409, not mystery rows.”*

## Kafka Event Backbone

KernelQ uses **Postgres** for durable job **state** and **Kafka** for durable job **handoff** from schedulers to executors. Today scheduler ticks **claim** jobs in Postgres (`queued` → `dispatched`); **publishing to Kafka is the next step**—each tick will eventually emit **dispatch events** so workers can pick up runnable work without the scheduler calling them directly.

**Why Kafka sits in the middle:**

- **Decouples scheduling from execution** — Python decides *what* to run and writes events; Go workers decide *when* they can run them (pull model).
- **Go workers consume from topics** — messages on **`kernelq.jobs.dispatch`** carry enough data for a worker to start (for example `job_id`, tenant, payload reference).
- **Buffering** — if workers are slow or briefly down, events wait in the log instead of blocking scheduler ticks or being lost.
- **Retries and replay** — failed consumption can be retried; separate retry or DLQ topics can align with `RETRY_SCHEDULED` and `DEAD_LETTERED` later.
- **Horizontal worker scaling** — add more Go consumer processes in a **consumer group**; Kafka partitions spread load without changing scheduler code.

Postgres remains the **system of record** for lifecycle; Kafka is the **event backbone** between “claimed for dispatch” and “running on a worker.” See `docs/decisions/ADR-0002-kafka-choice.md` for why Kafka was chosen over direct RPC or Redis queues.

**Simple flow (target architecture):**

```
Scheduler Tick
    ↓
Kafka Topic
    ↓
Go Workers
```

**Interview sound bite:** *“Ticks claim in Postgres, publish to Kafka, workers consume—state stays in the DB, transport stays in the log.”*

## Kafka Topics

KernelQ uses **four named topics** so normal work, retries, results, and permanent failures follow **separate lanes** instead of one mixed queue. Create them locally with `infra/kafka/create-topics.sh` (3 partitions each, replication factor 1 for dev).

### Topic roles

| Topic | Purpose | Who produces | Who consumes |
|-------|---------|--------------|--------------|
| **`kernelq.jobs.dispatch`** | **Normal runnable work** — after a scheduler tick claims a job in Postgres, the control plane publishes a dispatch event here for Go workers to pick up. | Python control plane (scheduler) | Go worker consumer group |
| **`kernelq.jobs.results`** | **Execution outcomes** — after a worker runs a job, it publishes a result event here so the control plane can update Postgres and drive retries. | Go workers | Python control plane (later) |
| **`kernelq.jobs.retry`** | **Retry flow** — jobs that **failed but can run again** (retries remain, backoff elapsed) are re-published here instead of competing with first-time dispatch traffic. Aligns with `RETRY_SCHEDULED` → back to `QUEUED` in the lifecycle. | Control plane / retry dispatcher | Go retry workers (or same workers, different consumer group) |
| **`kernelq.jobs.dlq`** | **Dead-letter queue (DLQ)** — **poison messages** (always crash the consumer) or jobs that **permanently failed** (max retries exhausted) land here for inspection, alerting, or manual replay—not for automatic execution. Aligns with `DEAD_LETTERED`. | Control plane / worker error handler | Ops tooling, dashboards, manual replay jobs |

**Plain English:** *dispatch* is “run this job”; *results* is “here’s what happened”; *retry* is “try again later”; *dlq* is “stop auto-running this and let a human look.”

### Why separate topics (not one big queue)

Mixing first-time dispatch, retries, and dead letters in a **single topic** makes operations harder:

- A **poison message** on the main lane can block or slow normal throughput.
- **Retry storms** after an outage spike traffic that looks like new work—harder to tune consumers and alerts.
- **Metrics and lag** become ambiguous: is high lag “busy” or “broken”?

Separate topics give **clear semantics**: monitor dispatch lag for capacity, retry lag for failure recovery, DLQ depth for jobs that need human attention.

### Partitions (parallel consumption)

Each topic is created with **3 partitions** locally. A **partition** is an ordered slice of the topic log. Kafka assigns partitions to consumers in a **consumer group** so **multiple Go worker processes can read in parallel**—worker A might handle partition 0, worker B partition 1, and so on.

- **More partitions** → more parallel consumption (up to one consumer per partition per group).
- **Same `job_id` keyed to one partition** (future design) → ordering per job stays predictable.

Partitions are how KernelQ scales workers **without** changing scheduler code: add consumers, Kafka spreads partition assignment.

### Replication factor (local vs production)

**Locally:** replication factor **1** is enough—one broker, one copy of each partition on disk. Fast to run in Docker Compose; no cross-broker redundancy.

**In production:** replication factor would be **3** (or higher per ops policy) so each partition is copied to multiple brokers. If one broker dies, another replica still serves reads and can become leader—**durability and availability** under real failures.

**Interview sound bite:** *“Four topics—dispatch, results, retry, DLQ—dispatch out, results back, retries and poison on their own lanes; three partitions for parallel consumers locally; replication 1 in dev, 3+ in prod.”*

## Kafka Dispatch Producer

The **Python control plane** publishes **dispatch events** to Kafka after it decides a job is ready for workers. Each event is a small JSON message (for example `job_id`, `tenant_id`, `priority`, `state`, `payload`) that tells executors *what to run* without the scheduler calling them directly.

**Where events go:** normal dispatch traffic is written to **`kernelq.jobs.dispatch`**. That is the happy-path topic created by `infra/kafka/create-topics.sh`. Retry and DLQ traffic use their own topics later.

**Who reads them:** **Go workers** (not built yet) will **consume** from `kernelq.jobs.dispatch` in a consumer group, transition jobs toward `running` in Postgres, and execute the work. Kafka carries the handoff; Postgres stays the system of record.

**Producer wrapper (`control_plane/kernelq/kafka_producer.py`):** scheduler code should not scatter raw `confluent_kafka` calls. A thin **`KafkaJobProducer`** + **`DispatchEvent`** dataclass centralize:

- topic name (`kernelq.jobs.dispatch`)
- JSON serialization
- message key (`job_id`, for partition stickiness)
- flush / shutdown behavior

The tick runner calls **`publish_dispatch_event(...)`** after **`claim_schedulable_jobs`** when a producer is configured. **Unit tests inject a fake producer** so pytest does not need a real broker.

**What exists today:** **`SchedulerTickRunner`** can publish after claim when **`job_producer`** is passed in. Production wiring (always-on producer in the tick loop, metrics, outbox) is still evolving—see **Scheduler to Kafka Dispatch Flow**.

**Interview sound bite:** *“Thin Python producer publishes JSON to `kernelq.jobs.dispatch`; the tick runner calls it after Postgres claim; Go workers consume later.”*

## Scheduler to Kafka Dispatch Flow

**`SchedulerTickRunner`** now connects **Postgres scheduling** to the **Kafka event backbone**. When you pass a **`KafkaJobProducer`** (or test fake), each tick **claims** runnable jobs, then **publishes** one **`DispatchEvent`** per claimed row.

**Step-by-step (one `run_once()`):**

```
queued rows in Postgres
    ↓  claim_schedulable_jobs (one transaction, FOR UPDATE SKIP LOCKED)
dispatched rows in Postgres
    ↓  for each claimed job: build DispatchEvent, publish_dispatch_event
messages on kernelq.jobs.dispatch
    ↓  (later) Go workers consume
running / terminal states in Postgres
```

1. **Claim in Postgres first** — `claim_schedulable_jobs` atomically moves **`queued` → `dispatched`**. This prevents two schedulers from picking the same row and gives a durable “we own this handoff” record.
2. **Publish to Kafka** — for each claimed job, the tick builds a **`DispatchEvent`** (`event_type`, `job_id`, `tenant_id`, `priority`, `state`, `payload`) and sends JSON to **`kernelq.jobs.dispatch`** (message **key** = `job_id`).
3. **Return counts** — **`SchedulerTickResult`** includes **`published_count`** and **`publish_errors`** alongside claim counts.

**Why this matters:** the Python control plane no longer stops at “marked dispatched in the database.” It can **hand work to the broker** so Go workers pull from the log instead of being called directly. Postgres = lifecycle truth; Kafka = transport between claim and execution.

**Optional producer:** pass **`job_producer=None`** to claim only (same behavior as before Kafka integration)—useful for tests and gradual rollout.

### Known reliability gap (claim before publish)

Today the order is **claim, then publish**. We do **not** roll back Postgres if Kafka fails.

| If this succeeds | But this fails | Problem |
|------------------|----------------|---------|
| DB row → **`dispatched`** | Kafka **`publish_dispatch_event`** | Job looks handed off in Postgres, but **no message** on `kernelq.jobs.dispatch`—workers never see it |

The tick records the failure in **`publish_errors`** and continues other jobs, but the stranded row stays **`dispatched`**. A later milestone will fix this with an **outbox-style pattern** (write event durably, then publish) or a **retryable dispatch mechanism** (reconcile / republish / revert state).

**Interview sound bite:** *“Claim in Postgres, publish to `kernelq.jobs.dispatch`, workers consume later—today claim-before-publish can strand rows if Kafka fails; outbox or retryable dispatch fixes that next.”*

## Manual End-to-End Dispatch Smoke Test

KernelQ can now **manually verify** the path from a **persisted `queued` job** to a **Kafka dispatch message** on your laptop—without Go workers running yet.

**What you are proving:** the **control plane handoff** works end to end: durable row in Postgres → one scheduler tick → JSON on **`kernelq.jobs.dispatch`**.

**What you are not proving:** full **worker integration** (execution, retries, exactly-once, production SLOs). Treat this as a **smoke test**, not a production readiness check.

**How it works (plain English):**

1. **Enqueue** a job (REST API or SQL) so Postgres has a row in state **`queued`**.
2. Run **`control_plane/scripts/run_scheduler_tick_once.py`** — one **`SchedulerTickRunner.run_once()`** with a **real `KafkaJobProducer`** (`localhost:9092`), claiming up to one job and publishing a **`DispatchEvent`**.
3. Read the topic with **`kafka-console-consumer`** inside the **`kernelq-kafka`** container — that CLI **stands in for future Go workers** today. If you see your `job_id` in JSON, the broker received what the tick sent.

```
API / SQL enqueue  →  Postgres (queued)
                         ↓
              run_scheduler_tick_once.py
                         ↓
              kernelq.jobs.dispatch
                         ↓
              kafka-console-consumer  (temporary stand-in for Go workers)
```

**Why a script instead of only unit tests:** pytest uses **fake producers** to test logic fast without a broker. The manual script uses **real Postgres + real Kafka**, so you catch wiring mistakes (bootstrap address, topic name, broker down) that fakes cannot.

**Step-by-step commands:** see **Manual Scheduler-to-Kafka Smoke Test** in `docs/deploy.md`.

**Interview sound bite:** *“We smoke-test queued → tick → dispatch topic with `run_scheduler_tick_once.py`; the Kafka CLI plays worker until Go consumes for real.”*

## Go Worker Plane

KernelQ now has a **Go worker-plane foundation** in the **`worker/`** directory—a separate codebase from the Python control plane, aligned with **ADR-0001** (Python decides, Go executes).

**What the control plane does today:** the Python scheduler **publishes dispatch events** to **`kernelq.jobs.dispatch`** after claiming jobs in Postgres (`SchedulerTickRunner` + `KafkaJobProducer`). Messages carry `job_id`, `tenant_id`, `priority`, `state`, and `payload`.

**What Go workers will do next:** **consume** those dispatch events from Kafka (consumer group on `kernelq.jobs.dispatch`), validate each task, run job logic with **bounded concurrency**, and **update Postgres** (`dispatched` → `running` → terminal states). Retry and DLQ topics (`kernelq.jobs.retry`, `kernelq.jobs.dlq`) come later.

**Why Go for workers (not Python):**

- **Concurrency** — goroutines handle many in-flight jobs without the Python GIL limiting parallel execution.
- **Efficient long-running processes** — workers are always-on consumers; Go’s small memory footprint and predictable latency suit high-throughput, 24/7 broker consumption.
- **Clear split** — Python owns policy, API, and scheduling; Go owns the hot execution path.

**What exists in `worker/` today (foundation only):**

| Piece | Status |
|-------|--------|
| `go.mod` | Go module scaffold |
| `internal/worker/task.go` | `Task` struct + `ValidateTask()` |
| `internal/worker/dispatch_event.go` | `DispatchEvent` + `ParseDispatchEvent` |
| `internal/worker/consumer.go` | `ConsumerRunner` message-processing boundary |
| `internal/worker/handler.go` | `DispatchEventHandler` event → task |
| `internal/worker/executor.go` | `Executor` interface (execution boundary) |
| `internal/worker/execution_result.go` | `ExecutionResult`, outcome status constants |
| `internal/worker/result_event.go` | `WorkerResultEvent` → **`kernelq.jobs.results`** |
| `internal/worker/result_producer.go` | `ResultProducer`, `RecordingResultProducer` |
| `internal/worker/kafka_result_producer.go` | `KafkaResultProducer` → **`kernelq.jobs.results`** |
| `internal/worker/kafka_consumer.go` | `KafkaConsumer` — broker record → `Message` |
| `internal/worker/dlq.go` | `DeadLetterEvent`, `DeadLetterProducer` boundary |
| `internal/worker/kafka_dlq_producer.go` | `KafkaDeadLetterProducer` → **`kernelq.jobs.dlq`** |
| `cmd/consumer/main.go` | Kafka consumer entrypoint (poll loop + DLQ publish) |
| Offset commits / Postgres updates | **Not built yet** |

**`cmd/consumer`** runs a long-lived poll loop on **`kernelq.jobs.dispatch`** via **`KafkaConsumer.Run`**. Offset commits and Postgres status updates come next.

**Interview sound bite:** *“Python publishes to `kernelq.jobs.dispatch`; Go polls continuously with context-based shutdown; bounded concurrency and real execution land in Executor next.”*

## Go Worker Consumer Boundary

The Go worker plane now has a **message-processing boundary** (`ConsumerRunner` in `worker/internal/worker/consumer.go`). It sits between **Kafka transport** and **job execution logic**.

**Flow (one message):**

```
Raw Kafka bytes (Message.Value)
    ↓  ParseDispatchEvent (JSON + validation)
DispatchEvent
    ↓  DispatchHandler.Handle(event)
Job execution logic (later)
```

1. **Parse** — raw message bytes become a typed **`DispatchEvent`** (`ParseDispatchEvent`).
2. **Validate** — reject malformed JSON, wrong `event_type`, blank ids, bad state, etc., before any handler runs.
3. **Handle** — pass valid events to a **`DispatchHandler`** implementation (real executor or test fake).

**Why separate this layer:** Kafka SDK code (poll, offsets, consumer groups) changes for different brokers and deployments. Parsing, validation, and execution rules should not live inside the transport loop. This boundary lets you test **“given these bytes, what happens?”** with fake messages—no broker required.

**Interview sound bite:** *“ConsumerRunner: bytes → parse/validate → handler—transport separate from execution; fake messages in tests, real Kafka client later.”*

## Worker Execution Boundary

The Go worker now **separates message processing from task execution**. Each layer has one job—easier to test, reason about, and extend.

**Layered flow:**

```
Message bytes
    ↓  ConsumerRunner.ProcessMessage
DispatchEvent (parsed + validated)
    ↓  DispatchEventHandler.Handle
Task (mapped + ValidateTask)
    ↓  Executor.Execute
ExecutionResult + error (outcome vs infra)
```

| Layer | Role |
|-------|------|
| **`ConsumerRunner`** | Parses and validates dispatch **messages** (`ParseDispatchEvent` on raw bytes). |
| **`DispatchEventHandler`** | Converts a validated **`DispatchEvent`** into a **`Task`** and validates again before run. |
| **`Executor`** | **Boundary where actual execution will happen**—run the job, enforce limits, report outcomes. |

**Why this split matters:** transport (Kafka), protocol (JSON contract), and execution (run payload, update Postgres) change at different rates. Keeping them separate means you can test **`DispatchEventHandler`** with a **fake executor** without a broker, and later add **bounded concurrency** and **worker metrics** inside **`Executor`** implementations without rewriting parse logic.

**What exists today:** interfaces and handlers only—tests use fake executors. **Real execution**, **concurrency caps**, and **status reporting to Postgres** come in later milestones.

**Interview sound bite:** *“ConsumerRunner parses messages; DispatchEventHandler maps to Task; Executor runs work—three layers so concurrency and metrics land in one place later.”*

## Worker Execution Result Model

Go workers now report **structured execution outcomes** via **`ExecutionResult`** (`worker/internal/worker/execution_result.go`). A plain Go **`error`** is not enough: the same `error` shape could mean “Postgres was down” or “job failed but retry later.” **`ExecutionResult`** separates those concerns.

**Two return values from `Executor.Execute`:**

| Return | Meaning | Example |
|--------|---------|---------|
| **`ExecutionResult`** | **Business execution outcome** — the job attempt finished and reported a status | succeeded, dependency timeout (retryable), invalid payload (terminal) |
| **`error`** | **Infrastructure failure** — execution could not complete reliably | Postgres unreachable, executor crashed before reporting outcome |

When **`error` is non-nil**, callers should **ignore** the **`ExecutionResult`** and treat the failure as operational (alert, reconnect, do not guess retry vs dead-letter from the error string alone).

**Status values (`ExecutionStatus`):**

| Status | Value | Meaning |
|--------|-------|---------|
| **`ExecutionSucceeded`** | `succeeded` | Job completed successfully → Postgres **`succeeded`** |
| **`ExecutionRetryableFailure`** | `retryable_failure` | Transient failure; another attempt may work → **`failed`** → **`retry_scheduled`** |
| **`ExecutionTerminalFailure`** | `terminal_failure` | Permanent failure or policy says stop → **`dead_lettered`** (and/or DLQ) |

**`DispatchEventHandler.Handle`** validates the executor’s result before returning it upstream. Invalid status values are rejected like bad JSON—executors must use the defined constants.

**How this connects to scheduler retries (future):**

```
Worker runs job → ExecutionRetryableFailure
    ↓  Postgres: running → failed
Scheduler / retry path: failed → retry_scheduled → (backoff) → queued
    ↓  publish to kernelq.jobs.retry
Worker consumes retry message → run again
```

**Future scheduler retry behavior will use `retryable_failure`**—not generic errors. The control plane already owns **`retry_count`**, **`max_retries`**, and transitions **`FAILED → RETRY_SCHEDULED → QUEUED`**. Workers supply the **outcome**; the scheduler supplies **policy** (when to re-queue, backoff, publish to **`kernelq.jobs.retry`**, or move to **`dead_lettered`** when retries are exhausted).

**`terminal_failure`** aligns with stop-auto-retry paths; **`succeeded`** aligns with normal completion. **Protocol/parse failures** on the dispatch topic remain separate (DLQ on **`kernelq.jobs.dlq`**)—they are not execution outcomes.

**Interview sound bite:** *“ExecutionResult is business outcome—succeeded, retryable_failure, terminal_failure; error is infra; scheduler retries use retryable_failure, not error strings.”*

## Worker Result Event Contract

KernelQ separates **work orders** from **completion reports** on Kafka:

```
Python control plane                    Go workers
        │                                      │
        │  DispatchEvent (job.dispatch)        │
        ├──────── kernelq.jobs.dispatch ──────►│ consume, execute
        │                                      │
        │  WorkerResultEvent (job.result)      │
        │◄──────── kernelq.jobs.results ───────┤ publish outcome
        │                                      │
   update Postgres job state              (later: result producer)
```

**Dispatch events flow control plane → workers.** After a scheduler tick claims a job, Python publishes **`DispatchEvent`** JSON to **`kernelq.jobs.dispatch`**—“run this job now.”

**Result events flow workers → control plane.** After execution, Go workers will publish **`WorkerResultEvent`** JSON to **`kernelq.jobs.results`**—“here’s what happened.”

**`WorkerResultEvent` (`worker/internal/worker/result_event.go`):**

| Field | Purpose |
|-------|---------|
| **`event_type`** | Always **`job.result`** |
| **`job_id`** | Which job finished (Kafka message key in future producer) |
| **`status`** | Outcome: **`succeeded`**, **`retryable_failure`**, or **`terminal_failure`** |
| **`message`** | Optional detail for logs and debugging (may be blank) |
| **`worker`** | Which consumer identity reported the outcome (for example **`kernelq-go-worker`**) |

**Status values match `ExecutionResult`**—same strings as **`ExecutionStatus`** in Go (`succeeded`, `retryable_failure`, `terminal_failure`). The in-process model and the on-wire event share one vocabulary so Python consumers do not reinterpret outcomes.

**`NewWorkerResultEvent`**, **`Validate()`**, and **`ToJSON()`** exist today; **Kafka publishing and control-plane consumption are not wired yet.**

**The control plane will later consume results and update job state:**

- **`succeeded`** → Postgres **`succeeded`**
- **`retryable_failure`** → **`failed`** → **`retry_scheduled`** (when retries remain)
- **`terminal_failure`** → **`dead_lettered`** (or policy-driven terminal handling)

Postgres stays the **system of record**; **`kernelq.jobs.results`** is the **feedback lane** that closes the loop after dispatch.

**Interview sound bite:** *“Dispatch out on kernelq.jobs.dispatch, results back on kernelq.jobs.results—WorkerResultEvent carries job_id, status, message, worker; status matches ExecutionResult; control plane updates Postgres later.”*

## Worker Result Producer Boundary

KernelQ splits **what workers report** from **how they send it**—the same pattern as **`DeadLetterProducer`** for DLQ and **`KafkaJobProducer`** on the Python side.

| Piece | Role |
|-------|------|
| **`WorkerResultEvent`** | **What** workers report—JSON contract (`job.result`, `job_id`, `status`, `message`, `worker`) |
| **`ResultProducer`** | **How** workers publish—`PublishResult(event WorkerResultEvent) error` |

**Flow (target):**

```
Executor.Execute → ExecutionResult
    ↓  NewWorkerResultEvent(job_id, result, worker)
WorkerResultEvent
    ↓  ResultProducer.PublishResult
kernelq.jobs.results (Kafka)
```

**What exists today:**

- **`RecordingResultProducer`** in `worker/internal/worker/result_producer.go` is a **fake / in-memory** implementation for tests.
- **`KafkaResultProducer`** is the real broker implementation—see **Kafka Result Publishing**.
- **`cmd/consumer`** wires **`KafkaResultProducer`** on **`DispatchEventHandler`**—see **Worker Execution-to-Result Flow**.

**Why separate execution from transport:**

- **Handler and executor** should not embed confluent-kafka-go calls—execution rules change at a different rate than broker wiring.
- **Tests** inject **`RecordingResultProducer`** or a **fake `KafkaProducerClient`**—no Docker Kafka required.
- **Production** uses **`KafkaResultProducer`** without rewriting **`DispatchEventHandler`** or **`Executor`**.

**Interview sound bite:** *“WorkerResultEvent is the payload; ResultProducer is the publish boundary—RecordingResultProducer in tests, KafkaResultProducer in production; handler publishes after Execute.”*

## Kafka Result Publishing

Go workers will publish **`WorkerResultEvent`** messages to **`kernelq.jobs.results`** after job attempts finish. That closes the loop opened by dispatch on **`kernelq.jobs.dispatch`**.

**End-to-end flow (target):**

```
kernelq.jobs.dispatch
    ↓  worker consumes, executes
ExecutionResult
    ↓  NewWorkerResultEvent(job_id, result, worker)
WorkerResultEvent
    ↓  KafkaResultProducer.PublishResult
kernelq.jobs.results
    ↓  (future) Python result consumer
Postgres job state updated
```

**`KafkaResultProducer`** (`worker/internal/worker/kafka_result_producer.go`) is the **real Kafka implementation** of the **`ResultProducer`** boundary:

1. **Validate** the **`WorkerResultEvent`**
2. **JSON-encode** via **`ToJSON()`**
3. **Produce** to **`kernelq.jobs.results`** with **`JobID`** as the Kafka message key
4. **Flush** synchronously (simple local-dev behavior for now)

**Result events carry execution outcomes back to the Python control plane**—not raw dispatch bytes, not DLQ poison records. Each message says *how the job finished* (`succeeded`, `retryable_failure`, `terminal_failure`) so the control plane can apply lifecycle policy.

**What is wired vs future:**

| Today | Later |
|-------|-------|
| **`DispatchEventHandler`** publishes via **`KafkaResultProducer`** after **`Execute`** | Result publish success/failure metrics |
| **`cmd/consumer`** wires producer + **`WorkerName`** at **`localhost:9092`** | Postgres **`running`** transitions from worker |
| Topic **`kernelq.jobs.results`** in `create-topics.sh` | **Python result consumer** reads topic and **updates Postgres** |

**The Python result consumer is still future work.** Go publishes outcomes to **`kernelq.jobs.results`**; the control plane does not yet subscribe or move rows to terminal states based on **`status`**.

**Interview sound bite:** *“KafkaResultProducer writes WorkerResultEvent to kernelq.jobs.results—handler publishes after Execute; Python consumer updates Postgres next.”*

## Worker Execution-to-Result Flow

The Go worker now connects **dispatch consumption** to **result publishing** in one handler path. This is the happy-path feedback loop—separate from **DLQ** routing for poison dispatch bytes.

**Step-by-step (one dispatch message):**

```
kernelq.jobs.dispatch
    ↓  KafkaConsumer consumes record
DispatchEvent (parse + validate)
    ↓  DispatchEventHandler.Handle
Task (map + ValidateTask)
    ↓  Executor.Execute
ExecutionResult (+ error for infra failures only)
    ↓  NewWorkerResultEvent(job_id, result, worker)
WorkerResultEvent
    ↓  ResultProducer.PublishResult (KafkaResultProducer in cmd/consumer)
kernelq.jobs.results
    ↓  (future) Python result consumer
Postgres job state updated
```

1. **Worker consumes dispatch event** — **`KafkaConsumer`** polls **`kernelq.jobs.dispatch`**, **`ConsumerRunner`** parses JSON into **`DispatchEvent`**.
2. **Handler converts to Task** — **`DispatchEventHandler`** maps fields and **`ValidateTask`** before execution.
3. **Executor returns `ExecutionResult`** — business outcome (`succeeded`, `retryable_failure`, `terminal_failure`); plain **`error`** means infrastructure failure only.
4. **Handler creates `WorkerResultEvent`** — **`NewWorkerResultEvent(event.JobID, result, workerName)`**; blank worker name defaults to **`kernelq-go-worker`**.
5. **`ResultProducer` publishes** — **`PublishResult`** writes JSON to **`kernelq.jobs.results`** with **`JobID`** as the Kafka key (when producer is configured).
6. **Control-plane result consumer is future work** — Python will read **`kernelq.jobs.results`**, validate **`job.result`**, and update Postgres (`succeeded`, `failed` → `retry_scheduled`, `dead_lettered`).

**Optional by design:** **`ResultProducer`** may be **nil** in tests so handler/executor logic runs without a broker. Production **`cmd/consumer`** passes **`KafkaResultProducer`**.

**Interview sound bite:** *“Consume dispatch, Task, Execute, WorkerResultEvent, PublishResult to kernelq.jobs.results—Python consumer closes the loop in Postgres later.”*

## Worker Result Smoke Test

KernelQ can now **verify the worker-side Kafka loop** on your laptop—without the Python API or Postgres in the path.

**What you are proving:** a **valid `DispatchEvent`** on **`kernelq.jobs.dispatch`** is **consumed by the Go worker**, executed (logging executor today), and the handler **publishes a `WorkerResultEvent`** to **`kernelq.jobs.results`** with the same **`job_id`**.

**What you are not proving:** **Postgres updates** or **Python control-plane result consumption**—that is **future work**. The smoke test stops at the results topic; rows in Postgres stay unchanged.

**How it works (plain English):**

1. Start **local Kafka** and create topics.
2. Build and run **`cmd/consumer`** in the background.
3. Produce one valid dispatch JSON (unique **`job_id`**, e.g. `day47-smoke-…`).
4. Read **`kernelq.jobs.results`** and confirm a result record with that **`job_id`**.

```
valid DispatchEvent  →  kernelq.jobs.dispatch
                              ↓
                       Go worker (consume + execute)
                              ↓
                       WorkerResultEvent  →  kernelq.jobs.results
                              ↓
                       (future) Python result consumer  →  Postgres
```

**Why a script instead of only unit tests:** Go tests use **fake producers and in-memory messages** to stay fast. **`worker/scripts/smoke_worker_result.sh`** uses **real Kafka** so you catch broker wiring mistakes (topic names, bootstrap address, consumer group) that mocks miss.

**Run from the repository root:**

```bash
./worker/scripts/smoke_worker_result.sh
```

**Step-by-step commands:** see **Worker Result Smoke Test** in `docs/deploy.md` and **`worker/README.md`**.

**Interview sound bite:** *“Smoke script proves dispatch → Go worker → kernelq.jobs.results with matching job_id; Python result consumer and Postgres are the next loop to close.”*

## Python Result Consumer Contract

**Go workers publish `WorkerResultEvent` messages to `kernelq.jobs.results`** when a job attempt finishes (`event_type`: **`job.result`**, plus **`job_id`**, **`status`**, **`message`**, **`worker`**).

The **Python control plane** now has a **matching parser and validator** in **`control_plane/kernelq/result_event.py`**: **`parse_worker_result_event`** decodes JSON and **`WorkerResultEvent.validate()`** checks required fields and allowed statuses (`succeeded`, `retryable_failure`, `terminal_failure`).

Together with the Go **`WorkerResultEvent`** in **`worker/internal/worker/result_event.go`**, this **completes the cross-language return contract**—the same JSON shape on the wire, validated on both sides before anyone trusts it.

**What is not wired yet:** a **Kafka result consumer** that subscribes to **`kernelq.jobs.results`**, parses events, and **updates Postgres job state** (for example **`succeeded` → SUCCEEDED**, retryable failures toward **RETRY_SCHEDULED**, terminal failures toward **DEAD_LETTERED**). That consumer is the next step to **close the loop** opened by dispatch on **`kernelq.jobs.dispatch`**.

**Interview sound bite:** *“Go publishes WorkerResultEvent to kernelq.jobs.results; Python parses and validates the same contract today; tomorrow’s consumer turns status into Postgres transitions.”*

## Python Result Consumer Boundary

KernelQ now has a **Python result consumer skeleton** in **`control_plane/kernelq/result_consumer.py`**. It is the control-plane mirror of the Go worker pattern: **one layer for bytes, another for business logic**.

**What it does today:**

```
ResultMessage (key + raw JSON bytes)
    ↓  parse_worker_result_event
WorkerResultEvent (validated)
    ↓  ResultHandler.handle
(state updates, metrics — your hook)
```

- **`ResultConsumerRunner.process_message`** turns a raw **`ResultMessage`** into a validated **`WorkerResultEvent`**.
- **Valid events** are passed to **`ResultHandler`** — a small interface where **Postgres transitions**, retries, and metrics will live later.
- **Invalid JSON or invalid fields** fail during parse/validate **before** any handler runs.

**Why separate transport from state updates:** Kafka client code (subscribe, poll, commit offsets) changes for different brokers and deployments. **Job state rules** should not be tangled inside that transport. Tests can call **`process_message`** with fake bytes and a **fake handler** without a running broker or database.

**Future work:** **real Kafka consumption** on **`kernelq.jobs.results`** and **Postgres updates** that map **`status`** to lifecycle transitions (**SUCCEEDED**, **FAILED** → **RETRY_SCHEDULED** / **DEAD_LETTERED**, etc.).

**Interview sound bite:** *“ResultConsumerRunner parses and validates; ResultHandler owns state—Kafka transport stays outside so tests and Postgres logic evolve independently.”*

## Result-to-State Handling

KernelQ now **maps worker result events into durable job state** in Postgres. **`ResultStateHandler`** (`control_plane/kernelq/result_handler.py`) takes a validated **`WorkerResultEvent`** and writes **`jobs.state`** through **`JobRepository.update_job_state_from_worker_result`**.

**Who owns what:**

- **Workers (Go)** execute jobs and **report outcomes on Kafka** (`kernelq.jobs.results`). They **do not** write lifecycle state to Postgres directly.
- **Control plane (Python)** owns **durable job state**—the system of record for orchestration, retries, and API visibility.

**Current mapping:**

| Worker `status` | Control-plane action (today) |
|-----------------|------------------------------|
| `succeeded` | **`update_job_state_from_worker_result`** → **`succeeded`** |
| `retryable_failure` | **`schedule_retry_from_worker_result`** → **`retry_scheduled`** or **`dead_lettered`** (see **Retryable Failure Handling**, **Max Retry Exhaustion**) |
| `terminal_failure` | **`update_job_state_from_worker_result`** → **`failed`** (**DEAD_LETTERED** later) |

**End-to-end path (when wired):**

```
kernelq.jobs.results → KafkaResultConsumer → ResultConsumerRunner → ResultStateHandler → Postgres jobs.state
```

**Interview sound bite:** *“Workers publish outcomes; control plane owns Postgres—succeeded and terminal_failure map directly; retryable_failure goes through schedule_retry_from_worker_result.”*

## Retryable Failure Handling

When a Go worker reports **`retryable_failure`**, the **Python control plane** applies **retry scheduling policy** in Postgres via **`schedule_retry_from_worker_result`**.

**Flow:**

```
WorkerResultEvent (status: retryable_failure)
    ↓  ResultStateHandler.handle
JobRepository.schedule_retry_from_worker_result(job_id)
    ↓
retry_count < max_retries  →  retry_count += 1, retry_after set, state = retry_scheduled
retry_count >= max_retries →  state = dead_lettered  (see Max Retry Exhaustion)
```

**What happens today:**

- **Workers classify** the outcome (`retryable_failure` = transient; may work on another attempt).
- **Control plane owns policy** — workers do not decide how many retries remain.

**What is still future work:** **backoff tuning**, **long-running scanner daemon**, **`kernelq.jobs.retry`** / DLQ publish for **`terminal_failure`**.

**Interview sound bite:** *“Worker says retryable_failure; control plane schedules RETRY_SCHEDULED or dead-letters when exhausted—RetryScanner requeues due rows to QUEUED.”*

## Max Retry Exhaustion

**Retryable worker failures do not retry forever.** Each job carries **`retry_count`** and **`max_retries`** in Postgres; the control plane enforces a **retry budget**.

**Policy (`schedule_retry_from_worker_result`):**

| Condition | Postgres state | Meaning |
|-----------|----------------|---------|
| **`retry_count < max_retries`** | **`RETRY_SCHEDULED`** | Another attempt is allowed after **`retry_after`**; **`retry_count`** increments |
| **`retry_count >= max_retries`** | **`DEAD_LETTERED`** | **Max retry exhaustion** — no automatic retries left |

**Why `DEAD_LETTERED` is terminal:**

- Stops **infinite retry loops** on work that will not succeed (poison payload, permanent dependency failure).
- Signals **operators** to inspect, fix root cause, or **manually replay** — not the normal scheduler path.
- Aligns with **`kernelq.jobs.dlq`** intent (Kafka DLQ publish for inspection is future work).

**`DEAD_LETTERED` jobs are not requeued** by **`RetryScanner`** — only **`RETRY_SCHEDULED`** rows with due **`retry_after`** move back to **`QUEUED`**.

**Interview sound bite:** *“retry_count vs max_retries bounds retries; budget left → RETRY_SCHEDULED; exhausted → DEAD_LETTERED terminal for ops inspection—not forever on the retry loop.”*

## Retry Requeue Scanner

After a **`retryable_failure`**, jobs sit in **`retry_scheduled`** until their **`retry_after`** timestamp passes. **`RetryScanner`** (`control_plane/kernelq/retry_scanner.py`) is the control-plane pass that **promotes due retries back into the normal queue**.

**Flow:**

```
retryable_failure (worker result)
    ↓  schedule_retry_from_worker_result
RETRY_SCHEDULED (retry_after set for wait)
    ↓  RetryScanner.run_once — requeue_due_retries(now)
QUEUED (ready for scheduler again)
    ↓  SchedulerTickRunner — claim + publish
kernelq.jobs.dispatch → Go worker → …
```

**What the scanner does:**

- **`JobRepository.requeue_due_retries(now, limit)`** — find **`retry_scheduled`** rows where **`retry_after <= now`**, atomically update to **`queued`**
- **`RetryScanner`** wraps one scan: records **`scanned_at`**, **`requeued_count`**, **`requeued_job_ids`**, and **`errors`**
- **Manual integration:** **`control_plane/scripts/run_retry_scanner_once.py`**

**Why back to `QUEUED` (not straight to dispatch):** the **same scheduler** that handles first-time work picks **`queued`** rows by priority and FIFO. Retries re-enter the **standard dispatch path**—no separate worker-side requeue logic.

**What is still future work:**

- **Dead-letter Kafka publish** — **`terminal_failure`**, poison jobs, **`kernelq.jobs.dlq`**
- **Long-running scanner loop** with metrics and graceful shutdown
- **Backoff tuning** when **`retry_after`** is computed at schedule time

**Interview sound bite:** *“Retryable failure → RETRY_SCHEDULED with retry_after; RetryScanner moves due jobs to QUEUED; scheduler dispatches them—DEAD_LETTERED jobs stay out of the scanner.”*

## Retry Requeue Smoke Test

KernelQ can now **verify retry state movement** on a laptop with **`./control_plane/scripts/smoke_retry_requeue.sh`**—Postgres and control-plane Python only (no Go worker execution required for this check).

**What the smoke test proves (retry loop shape):**

```
dispatched test job
    ↓  ResultStateHandler + retryable_failure (injected)
RETRY_SCHEDULED
    ↓  RetryScanner (retry_after made due)
QUEUED
    ↓  SchedulerTickRunner — one tick
DISPATCHED (again)
```

**Step by step:**

1. **Retryable worker result** (simulated) → **`schedule_retry_from_worker_result`** → **`RETRY_SCHEDULED`**
2. **`RetryScanner`** → due rows (**`retry_after <= now`**) → **`QUEUED`**
3. **Scheduler tick** → claim + publish → **`DISPATCHED`** on **`kernelq.jobs.dispatch`**

**What this proves:** the **retry loop shape** works end-to-end in Postgres and through the **same dispatch path** as first-time jobs. Retries are not a separate ad-hoc pipeline—they re-enter **`queued`** and get picked up by **`SchedulerTickRunner`**.

**What it does not prove yet:** **real worker failure** on Kafka, **DLQ Kafka publish**, or **backoff tuning**.

**Interview sound bite:** *“Smoke script: retryable result → RETRY_SCHEDULED, scanner → QUEUED, scheduler → DISPATCHED again—that’s the retry loop shape.”*

## Retry Exhaustion Smoke Test

KernelQ can now **verify that exhausted retryable failures become `DEAD_LETTERED`** with **`./control_plane/scripts/smoke_retry_exhaustion.sh`**—Postgres and control-plane Python only (no Go worker).

**What the smoke test proves (exhaustion boundary):**

```
dispatched test job (retry_count = max_retries)
    ↓  ResultStateHandler + retryable_failure (injected)
DEAD_LETTERED
    ↓  RetryScanner — one scan
DEAD_LETTERED (unchanged — not requeued)
```

**Step by step:**

1. Create a **dispatched** job with **`retry_count = max_retries`** (budget already used)
2. **Inject `retryable_failure`** → **`schedule_retry_from_worker_result`** → **`DEAD_LETTERED`**
3. Run **`RetryScanner`** once — state **stays `DEAD_LETTERED`**

**Why this matters:** **`RetryScanner`** only requeues **`RETRY_SCHEDULED`** rows with due **`retry_after`**. It **must not** pick up **`DEAD_LETTERED`** jobs—that would **defeat max-retry policy** and risk **infinite retry loops** on poison or permanently failing work.

**Interview sound bite:** *“Exhaustion smoke: retry_count at max + retryable_failure → DEAD_LETTERED; scanner leaves it alone—terminal state protects the system from retrying forever.”*

## Dead-Lettered Job Inspection

**`DEAD_LETTERED` jobs are terminal durable records in Postgres** — the control plane’s system of record for work that will not auto-retry. Operators need a way to **review** them without ad hoc SQL.

**Inspection path today:**

```
jobs.state = dead_lettered (Postgres)
    ↓  JobRepository.list_dead_lettered_jobs
list_dead_lettered_jobs.py — operator-readable listing
```

Manual run from repo root:

```bash
PYTHONPATH=. python3 control_plane/scripts/list_dead_lettered_jobs.py
```

**Postgres vs Kafka DLQ:** this is **separate from `kernelq.jobs.dlq`**. DLQ holds **bad Kafka messages** (malformed dispatch, poison records). **`DEAD_LETTERED`** in Postgres holds **job lifecycle state** after retry exhaustion or (eventually) permanent failure policy. Both are for inspection; they answer different questions.

**Interview sound bite:** *“DEAD_LETTERED is durable Postgres state for ops review; list script reads it—Kafka DLQ is a different lane.”*

## Manual Dead-Letter Requeue

**`DEAD_LETTERED` jobs are terminal by default** — **`RetryScanner`** and automatic retry policy do not touch them. After **inspection** (`list_dead_lettered_jobs.py`), operators may **manually requeue** when root cause is fixed.

**Manual requeue path:**

```
DEAD_LETTERED (terminal)
    ↓  JobRepository.requeue_dead_lettered_job (operator action)
QUEUED
    ↓  SchedulerTickRunner — normal dispatch
DISPATCHED → worker → …
```

**Policy:**

- Only rows currently **`dead_lettered`** are updated — missing or wrong-state jobs are rejected
- **`retry_count` is preserved** — exhaustion history stays visible for audit
- **`retry_after`** is cleared — the job is not a delayed **`RETRY_SCHEDULED`** row
- **No Kafka publish in the requeue script** — the **scheduler** picks **`queued`** jobs on the next tick, same as first-time work

```bash
PYTHONPATH=. python3 control_plane/scripts/requeue_dead_lettered_job.py <job_id>
```

**Interview sound bite:** *“DEAD_LETTERED is terminal unless ops explicitly requeues—DEAD_LETTERED → QUEUED, retry_count kept, scheduler dispatches normally later.”*

## Kafka Result Consumer Skeleton

The **Python control plane** can now **consume from `kernelq.jobs.results`** using **`KafkaResultConsumer`** (`control_plane/kernelq/kafka_result_consumer.py`). This connects **worker result events** on Kafka to **durable state update logic** in Postgres.

**Layered flow (one record):**

```
kernelq.jobs.results (confluent_kafka.Message)
    ↓  KafkaResultConsumer.process_kafka_message / poll_once
ResultMessage (key + JSON bytes)
    ↓  ResultConsumerRunner.process_message
WorkerResultEvent (validated)
    ↓  ResultStateHandler.handle
Postgres jobs.state
```

**Why the layers stay separate:** **`KafkaResultConsumer`** owns **broker transport** (subscribe, poll, close). **`ResultConsumerRunner`** owns **parse + validate**. **`ResultStateHandler`** owns **Postgres updates**. Tests inject **fake Kafka messages** so each layer can be verified without a running broker.

**What exists today:** **`poll_once`** reads **at most one message** per call. Manual integration: **`control_plane/scripts/consume_result_once.py`** (10s timeout, prints whether a message was processed).

**Future work:** a **long-running daemon loop** (continuous poll, graceful shutdown, offset commits, metrics)—same pattern as the Go worker poll loop on dispatch.

**Interview sound bite:** *“Python polls kernelq.jobs.results once—KafkaResultConsumer → runner → ResultStateHandler → Postgres; daemon loop and retry policy come next.”*

## Full Completion Loop

KernelQ now has a **full feedback loop** for successful jobs—the **MVP architecture** proven end-to-end on a laptop:

```
queued job (Postgres)
    ↓  SchedulerTickRunner — claim + publish
kernelq.jobs.dispatch (Kafka)
    ↓  Go worker — consume, execute
kernelq.jobs.results (Kafka) — WorkerResultEvent
    ↓  KafkaResultConsumer → ResultConsumerRunner → ResultStateHandler
succeeded (Postgres jobs.state)
```

**What this proves:** the control plane can **admit and dispatch** work, workers can **execute and report outcomes** on Kafka, and the control plane can **close the loop** by updating **durable job state** in Postgres. Clients and operators see a job move from **queued** to **succeeded** without manual SQL or one-off Kafka producers.

**Who owns what (unchanged):**

- **Python control plane** — scheduling, claiming, dispatch publish, result consumption, Postgres as system of record.
- **Go workers** — high-throughput execution; publish structured results; **do not** write lifecycle state directly.
- **Kafka** — durable transport between dispatch and results; buffers decouple planes.

**How to verify locally:** **`./control_plane/scripts/smoke_full_completion.sh`** (see `docs/deploy.md`).

**What still comes later:** **backoff tuning** and **long-running scanner/consumer daemons**, **dead-letter policy** (`terminal_failure` → **`DEAD_LETTERED`** / **`kernelq.jobs.dlq`**), and richer **`RUNNING`** transitions. The **happy path**, **retry scheduling**, and **one-shot retry requeue** work today; fully automatic retry loops do not.

**Interview sound bite:** *“Queued in Postgres, dispatched on Kafka, executed in Go, result back on Kafka, state updated in Python—that’s the MVP loop; backoff, requeue, and DLQ are the next layer.”*

## Worker Kafka Consumption

The **Go worker plane** now **owns Kafka consumption** on **`kernelq.jobs.dispatch`**. After the Python control plane claims jobs and publishes **`DispatchEvent`** JSON, Go workers pull records from the broker and route them into the existing processing stack.

**Transport vs parsing:** **`KafkaConsumer`** (confluent-kafka-go) handles **broker transport only**—topic subscription, consumer group, record keys and values. It does **not** parse JSON or execute jobs. Each `*kafka.Message` becomes an in-memory **`Message`** (`Key`, `Value`) and is passed to **`ConsumerRunner.ProcessMessage`**.

**Layered flow (one record):**

```
kernelq.jobs.dispatch (*kafka.Message)
    ↓  KafkaConsumer.ProcessKafkaMessage
ConsumerRunner Message (Key + Value bytes)
    ↓  ConsumerRunner.ProcessMessage (parse + validate)
DispatchEvent
    ↓  DispatchEventHandler.Handle (execution-independent)
Task
    ↓  Executor.Execute
Job execution (later)
```

**Why this split matters in interviews:**

- **Go owns consumption** — workers pull from the log; Python does not push directly to worker processes.
- **Kafka transport is separate from message parsing** — SDK and broker config can change without touching `ParseDispatchEvent` or handlers.
- **Kafka messages become `ConsumerRunner` messages** — one thin adapter (`ProcessKafkaMessage`) bridges confluent-kafka-go to the testable in-memory boundary.
- **`DispatchEventHandler` stays execution-independent** — it maps validated events to **`Task`** values without caring whether bytes came from Kafka or a unit test fake.
- **`Executor` still owns execution logic** — running work, concurrency caps, timeouts, and Postgres status updates belong there—not in the Kafka client or parser.

**What exists today:** **`KafkaConsumer`**, **`Run`** poll loop, in-memory tests, and **`cmd/consumer`** (subscribe, poll, graceful shutdown). **Offset commit policy**, **retries**, and **real execution** come in later milestones.

**Interview sound bite:** *“Go owns consumption: KafkaConsumer turns broker records into ConsumerRunner messages; parsing and execution stay in separate layers so tests skip the broker and execution can evolve independently.”*

## Worker Poll Loop and Graceful Shutdown

The Go worker can now run as a **long-lived process** that **polls Kafka** for dispatch events instead of subscribing once and exiting.

**What the poll loop does:**

```
SIGINT / SIGTERM
       ↓
context canceled
       ↓
KafkaConsumer.Run(ctx)  ←── Poll(1000ms) in a loop
       ↓
*kafka.Message → ProcessKafkaMessage → ConsumerRunner → handler → executor
       ↓
close poller, return nil
```

1. **`cmd/consumer`** wires the stack: broker client → **`KafkaConsumer`** → **`ConsumerRunner`** → **`DispatchEventHandler`** → **`Executor`** (logging no-op today).
2. **`Run(ctx, pollTimeoutMs)`** calls **`Poll`** repeatedly on **`kernelq.jobs.dispatch`**.
3. Each **`*kafka.Message`** is handed to **`ProcessKafkaMessage`**—same path as unit tests, but driven by the broker.
4. The loop keeps running until **`ctx`** is canceled.

**Graceful shutdown with `context.Context`:**

- **`signal.NotifyContext`** listens for **SIGINT** (Ctrl+C) and **SIGTERM** (container stop).
- When the signal arrives, **`ctx`** is canceled.
- **`Run`** stops polling, **closes the Kafka consumer**, and returns **`nil`**—no abrupt exit mid-setup.
- **`cmd/consumer`** prints a stopped message so operators know shutdown completed cleanly.

**Why this matters for later work:**

- **Long-running execution** — workers stay up and drain the dispatch topic as the Python scheduler publishes.
- **Bounded concurrency (next)** — the poll loop can stay thin; a future **`Executor`** can enqueue tasks to a fixed-size worker pool without rewriting Kafka code.
- **Production readiness** — graceful shutdown avoids leaving consumer groups in a bad state and sets the pattern for **draining in-flight jobs** before exit.

**What is not built yet:** explicit **offset commits**, **Postgres status updates**, and **concurrency limits**. Invalid dispatch messages are tolerated (see **Worker Invalid Message Handling**); **Kafka broker errors** still stop the loop.

**Interview sound bite:** *“The worker polls Kafka in a loop, cancels via context on SIGTERM, closes the consumer cleanly, and keeps transport separate so bounded concurrency lands in Executor later—not in the poll loop.”*

## Worker Invalid Message Handling

The Go worker now **tolerates invalid dispatch messages** instead of crashing the whole process when one record is bad.

**What happens today (one bad record on `kernelq.jobs.dispatch`):**

```
*kafka.Message
    ↓  ProcessKafkaMessage (parse / validate / handle)
error returned
    ↓
ConsumerStats.MessageErrors++
    ↓
keep polling (do not exit Run)
```

- **Bad JSON**, **validation failures** (wrong `event_type`, blank `job_id`, bad `state`), and **handler errors** increment **`MessageErrors`** and the worker **keeps polling**.
- **`ConsumerStats`** also tracks **`MessagesSeen`**, **`MessagesProcessed`**, and **`KafkaErrors`**—printed on clean shutdown from **`cmd/consumer`**.

**Why one malformed message must not kill the worker:**

- A **poison message** is a record that will **keep failing** if you retry it the same way.
- If the worker **exits on first failure**, every job **behind** that offset waits while the process is down—**head-of-line blocking** and growing **consumer lag**.
- Production workers should stay **available** for healthy messages even when **some** records are bad.

**What is still fatal today:** **`kafka.Error`** events from the broker (connection problems, etc.) still **return from `Run`** and stop the process—infra failures are treated differently from bad payloads.

**Future work — DLQ:** see **Kafka DLQ Publishing** (real broker publish wired in **`cmd/consumer`**).

**Interview sound bite:** *“Invalid dispatch messages increment MessageErrors and the poll loop continues; one poison record doesn’t kill the worker—DLQ routing preserves failure evidence.”*

## Dead Letter Queue Boundary

KernelQ defines a dedicated DLQ topic: **`kernelq.jobs.dlq`**. It is the **failure lane** for messages that should **not** keep retrying on **`kernelq.jobs.dispatch`** (see **Kafka Topics**).

**Why a DLQ exists:**

- **Dispatch** is the happy path—runnable work for Go workers.
- **DLQ** holds **malformed or poison messages** and (later) permanently failed jobs so ops can **inspect, alert, and replay** without blocking healthy traffic.

**`DeadLetterEvent` (Go worker contract today):**

The worker plane defines **`DeadLetterEvent`** in `worker/internal/worker/dlq.go`. Each record preserves **failure evidence** instead of silently dropping bad bytes:

| Field | Purpose |
|-------|---------|
| **`reason`** | Why processing failed (parse error, validation, handler failure, …) |
| **`source_topic`** | Where the message came from (usually **`kernelq.jobs.dispatch`**) |
| **`worker`** | Which consumer identity saw the failure (for example **`kernelq-worker`**) |
| **`original_key`** | Kafka message key when present (often `job_id`; may be blank) |
| **`original_value`** | Raw message body as received—enables debugging and replay |

**`DeadLetterProducer`** is the publish boundary (`PublishDeadLetter`). See **Worker DLQ Routing Boundary** for how **`KafkaConsumer`** uses it at runtime.

**How this connects to invalid-message handling:**

```
*kafka.Message on kernelq.jobs.dispatch
    ↓  ProcessKafkaMessage fails
MessageErrors++
    ↓  (when DeadLetterProducer is set) DeadLetterEvent → PublishDeadLetter
keep polling
```

Day 38 stopped poison messages from **killing the worker**. The DLQ boundary stops them from **disappearing**—operators get a durable record on a separate topic.

**What exists today vs later:**

| Today | Later |
|-------|-------|
| `DeadLetterEvent` + `Validate()` + `ToJSON()` | DLQ metrics and alerting |
| `KafkaDeadLetterProducer` → **`kernelq.jobs.dlq`** | Offset commit policy after DLQ publish |
| Fake producer clients in tests | Replay tooling and ops dashboards |

**Interview sound bite:** *“kernelq.jobs.dlq is the failure lane; DeadLetterEvent stores reason, source topic, worker, and original key/value—KafkaDeadLetterProducer writes them to the broker.”*

## Worker DLQ Routing Boundary

Invalid-message handling in **`KafkaConsumer.Run`** is now **connected to `DeadLetterProducer`**. When **`ProcessKafkaMessage`** fails, the worker does not exit—it records the failure and optionally routes it to DLQ logic.

**What happens on a bad dispatch message:**

1. **`MessageErrors++`** — same Day 38 tolerance; polling continues.
2. If **`DeadLetterProducer` is configured** — build a **`DeadLetterEvent`**:
   - **`reason`** — the processing error string (parse, validation, handler, …)
   - **`original_key`** / **`original_value`** — the Kafka record as received
   - **`source_topic`** — from message metadata, or **`unknown`**
   - **`worker`** — **`kernelq-go-worker`**
3. Call **`PublishDeadLetter(event)`**.
4. **`DeadLettersPublished++`** on success, **`DeadLetterPublishErrors++`** on failure.
5. **Keep polling** — healthy messages still process.

**Why the producer is an interface:**

- **Tests** inject a **fake `DeadLetterProducer`** that captures events—no Kafka broker required.
- **Transport stays separate** from the poll loop—same pattern as **`KafkaPoller`** and Python’s **`KafkaJobProducer`**.
- **`KafkaDeadLetterProducer`** implements real broker publish (see **Kafka DLQ Publishing**).

**What is not built yet:**

- **Offset commits** after DLQ publish.
- **DLQ publish retries** beyond synchronous produce + flush.

**Interview sound bite:** *“On ProcessKafkaMessage failure, MessageErrors++ and PublishDeadLetter with reason plus original payload—poll loop never stops on bad JSON.”*

## Kafka DLQ Publishing

Go workers **consume** runnable jobs from **`kernelq.jobs.dispatch`**. When a record cannot be parsed, validated, or handled, the worker builds a **`DeadLetterEvent`** and **`KafkaDeadLetterProducer`** writes it to **`kernelq.jobs.dlq`**.

**End-to-end flow:**

```
kernelq.jobs.dispatch (*kafka.Message)
    ↓  KafkaConsumer.Run → ProcessKafkaMessage fails
DeadLetterEvent { reason, original_key, original_value, source_topic, worker }
    ↓  KafkaDeadLetterProducer.PublishDeadLetter
kernelq.jobs.dlq (JSON on broker)
    ↓
keep polling dispatch topic
```

**What `KafkaDeadLetterProducer` does (`worker/internal/worker/kafka_dlq_producer.go`):**

1. **Validate** the **`DeadLetterEvent`**.
2. **JSON-encode** the event.
3. **Produce** to **`kernelq.jobs.dlq`** with **`OriginalKey`** as the Kafka message key.
4. **Flush** synchronously (simple local-dev behavior for now).

**Why this matters:**

- **Preserves original payload** — **`original_value`** is the raw dispatch body for replay and drift debugging.
- **Preserves failure reason** — **`reason`** explains parse vs validation vs handler failure.
- **Separates lanes** — poison traffic leaves **`kernelq.jobs.dispatch`** without stopping healthy consumption.

**DLQ publish failures are counted separately:**

- **`DeadLettersPublished`** — broker accepted the DLQ record.
- **`DeadLetterPublishErrors`** — validate, JSON, produce, or flush failed; **target should be 0** in healthy operation.
- Processing still failed (**`MessageErrors++`**); a publish error means evidence may **not** be on **`kernelq.jobs.dlq`**.

**`cmd/consumer`** wires **`NewKafkaDeadLetterProducer("localhost:9092")`** into **`KafkaConsumer.DeadLetterProducer`**. Tests use a **fake `KafkaProducerClient`**—no broker required.

**Interview sound bite:** *“Consume dispatch, on failure build DeadLetterEvent with reason and original bytes, KafkaDeadLetterProducer writes to kernelq.jobs.dlq—DeadLetterPublishErrors separate from MessageErrors.”*

## Cross-Language Event Contract

KernelQ uses one dispatch message shape across both languages.

- The **Python control plane** publishes **`DispatchEvent`** JSON to Kafka.
- The **Go worker plane** parses the same JSON into Go **`DispatchEvent`** structs.

That shared shape is a **contract** between planes: Python promises what fields it sends, and Go promises how it reads them.

**Validation in both places:**

- **Python** validates before publish (for example required fields and dispatch state).
- **Go** validates after parse (`ParseDispatchEvent` + `ValidateDispatchEvent`) before execution.

This two-sided validation reduces accidental **protocol drift** (for example renamed fields, wrong state values, missing IDs) and makes failures visible early instead of silently executing bad data.

**Interview sound bite:** *“One JSON contract, two implementations: Python publishes it, Go parses and validates it—double validation catches drift before it becomes production behavior.”*

## Data Flow

1. **Enqueue**: Client sends job request via REST API to control plane
2. **Persist**: Control plane saves job definition and schedule to Postgres
3. **Enqueue to Kafka**: When job is ready to run, control plane publishes to Kafka
4. **Workers consume**: Go workers pull jobs from Kafka
5. **Report back**: Workers update job state in Postgres and send metrics to control plane
6. **Completion**: Control plane updates final state and triggers any dependent jobs

## Job Lifecycle State Machine

### Why KernelQ Needs a Strict Job Lifecycle

A strict job lifecycle ensures that every job follows a predictable path from creation to completion. This prevents jobs from getting stuck in undefined states, makes debugging easier, and ensures the system behaves correctly even when things go wrong.

Without a strict lifecycle, jobs could be in ambiguous states like "maybe running" or "probably failed." This makes it impossible to know what's happening, retry correctly, or clean up resources. The state machine enforces rules about what can happen next, making the system reliable and predictable.

### Job States

KernelQ defines the following job states:

- **CREATED**: Job has been submitted via API but not yet scheduled. This is the initial state when a job is first created.

- **QUEUED**: Job is scheduled and waiting in the queue to be picked up by a worker. The scheduler has determined it's time to run, but no worker has claimed it yet.

- **DISPATCHED**: Job has been sent to Kafka and is available for workers to consume. A worker may pick it up soon.

- **RUNNING**: A worker has claimed the job and is currently executing it. The job code is actively running.

- **SUCCEEDED**: Job completed successfully. This is a terminal state.

- **FAILED**: Job failed during execution. This state can transition to RETRY_SCHEDULED (if retries remain) or DEAD_LETTERED (if no retries remain). It is not a terminal state.

- **RETRY_SCHEDULED**: Job failed but will be retried. The system has scheduled a retry attempt for a future time.

- **DEAD_LETTERED**: Job failed permanently after all retries were exhausted. It has been moved to a dead letter queue for manual inspection. This is a terminal state.

- **CANCELED**: Job was explicitly canceled by a user or system before completion. This is a terminal state.

### Terminal States

Terminal states are states that a job cannot leave once entered. These are:
- **SUCCEEDED**
- **DEAD_LETTERED**
- **CANCELED**

Note: **FAILED** is not a terminal state because it can transition to RETRY_SCHEDULED or DEAD_LETTERED.

Once a job reaches a terminal state, no further state transitions are allowed. The job's lifecycle is complete.

### Allowed Transitions

The state machine allows these transitions:

```
CREATED → QUEUED
CREATED → CANCELED

QUEUED → DISPATCHED
QUEUED → CANCELED

DISPATCHED → RUNNING
DISPATCHED → QUEUED (if dispatch fails or times out)

RUNNING → SUCCEEDED
RUNNING → FAILED
RUNNING → CANCELED

FAILED → RETRY_SCHEDULED (if retries remaining)
FAILED → DEAD_LETTERED (if no retries remaining)

RETRY_SCHEDULED → QUEUED (when retry time arrives)
RETRY_SCHEDULED → CANCELED
```

### Invalid Transitions

These transitions are not allowed and will be rejected:

- Any transition from a terminal state (SUCCEEDED, DEAD_LETTERED, CANCELED) is invalid.
- CREATED → RUNNING (must go through QUEUED and DISPATCHED first)
- QUEUED → SUCCEEDED (must be executed first)
- SUCCEEDED → any other state
- FAILED → RUNNING (must go through RETRY_SCHEDULED first)
- DISPATCHED → SUCCEEDED (must be RUNNING first)

### Where Retries Fit

When a job in the **RUNNING** state fails, it first transitions to **FAILED**. That keeps failure explicit and easier to measure.

From **FAILED**, the system checks if retries are configured and available:

1. If retries remain: **FAILED → RETRY_SCHEDULED**
2. If no retries remain: **FAILED → DEAD_LETTERED**

When a job is in **RETRY_SCHEDULED**, it waits for the retry delay (with exponential backoff and jitter). Once the delay expires, it transitions back to **QUEUED**, then follows the normal flow: QUEUED → DISPATCHED → RUNNING.

This creates a retry loop: **RUNNING → FAILED → RETRY_SCHEDULED → QUEUED → DISPATCHED → RUNNING** (repeat until success or max retries).

### Where Cancellation Fits

Cancellation can happen from any non-terminal state:

- **CREATED**: Cancel before scheduling
- **QUEUED**: Cancel before dispatch
- **DISPATCHED**: Cancel before worker picks it up
- **RUNNING**: Cancel during execution (worker must handle cancellation signal)
- **RETRY_SCHEDULED**: Cancel before retry executes

Once canceled, the job transitions to CANCELED (terminal state). Workers must check for cancellation signals periodically and stop execution gracefully.

### State Machine Diagram

```
                 ┌─────────┐
                 │ CREATED │
                 └────┬────┘
                      │
                      ▼
                 ┌────────┐
                 │ QUEUED │
                 └───┬─┬──┘
                     │ │
                     │ └──────────────► ┌──────────┐
                     │                  │ CANCELED │
                     │                  └──────────┘
                     ▼
              ┌────────────┐
              │ DISPATCHED │
              └─────┬──────┘
                    │
                    ▼
               ┌─────────┐
               │ RUNNING │
               └─┬───┬───┘
                 │   │
      ┌──────────┘   └───────────────┐
      ▼                              ▼
┌───────────┐                  ┌──────────┐
│ SUCCEEDED │                  │  FAILED  │
└───────────┘                  └────┬─────┘
                                    │
                     ┌──────────────┴──────────────┐
                     ▼                             ▼
            ┌─────────────────┐           ┌───────────────┐
            │ RETRY_SCHEDULED │           │ DEAD_LETTERED │
            └────────┬────────┘           └───────────────┘
                     │
                     ▼
                 ┌────────┐
                 │ QUEUED │
                 └────────┘
```

Legend:

- Solid arrows: normal transitions
- Terminal states: **SUCCEEDED**, **DEAD_LETTERED**, **CANCELED**
