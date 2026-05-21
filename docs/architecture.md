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

KernelQ now has a **scheduler tick runner** in the **Python control plane** (`control_plane/kernelq/scheduler_tick.py`). A **tick** is one pass of the dispatch loop: “ask Postgres who is waiting, pick up to *N* jobs, and claim them.” This wires the repository query path into something you can call repeatedly from a future loop, cron job, or background task—still **synchronous** and **simple** today (no async, no Kafka yet).

**What happens in one tick (`SchedulerTickRunner.run_once()`):**

1. **Atomically claim jobs** — `claim_schedulable_jobs(limit=max_jobs_per_tick)` selects up to *N* **`queued`** rows (ordered by **`priority DESC`**, then **`created_at ASC`**) and marks them **`dispatched`** in **one transaction** (see **Atomic Job Claiming** below).
2. **Cap batch size** — `max_jobs_per_tick` limits how many rows one pass can claim so a tick cannot drain an unbounded backlog or overload downstream systems later.
3. **Return a summary** — **`SchedulerTickResult`** reports how many jobs were claimed, their `job_id`s, and any repository-level errors. With atomic claiming, **selected** and **dispatched** counts match on success, and **skipped** is zero unless the design changes.

**What `dispatched` means today:** the scheduler **selected and claimed** the job in Postgres. It does **not** yet mean “published to Kafka” or “running on a Go worker.” Think of it as *handed off from the waiting line to the outbound lane*—the durable record that this tick owns the job and another tick should not pick it again.

**What comes later:** the same tick will likely **publish selected jobs to Kafka** (before or while updating dispatch state), so workers can consume runnable work. The order might be: select → publish → mark `dispatched` (or publish only after a successful claim)—exact wiring is a later milestone. Today the tick stops at **Postgres claims** so selection and broker integration can be built and tested in steps.

**Where it lives:** the tick runner belongs in the **Python control plane** alongside `JobRepository` and the lifecycle state machine. **Go workers** execute jobs; they do not run this loop. In interviews: *“Control plane ticks read and claim queued rows; workers execute after Kafka delivers work.”*

**Interview sound bite:** *“Each tick calls `claim_schedulable_jobs` with a limit. `dispatched` today means claimed in Postgres, not yet on Kafka—that publish step is next.”*

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
