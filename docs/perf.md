# Performance

## Baseline Metrics Plan

| Metric | What it means | How we'll measure it | Why it matters | Target |
|--------|---------------|----------------------|----------------|--------|
| Throughput (tasks/sec) | How many tasks finish each second | Count completed tasks per second | Shows if the system can handle your workload | |
| End-to-end latency p50/p95/p99 (enqueue→completion) | How long tasks take from start to finish. p50 is the middle time, p95 is slower than 95% of tasks, p99 is slower than 99% | Time from when a task enters the queue until it finishes | Users care about speed. High latency means slow tasks | |
| Success rate (%) | What percent of tasks finish without errors | Count successful tasks divided by total tasks | Shows if the system is reliable | |
| Error rate (%) | What percent of tasks fail | Count failed tasks divided by total tasks | High error rate means something is broken | |
| Retry rate (%) | What percent of tasks need to run again after failing | Count retried tasks divided by total tasks | Shows how often things fail the first time | |
| DLQ rate (%) | What percent of tasks go to the dead letter queue after all retries fail | Count DLQ tasks divided by total tasks | Tasks in DLQ need manual attention | |
| Duplicate execution rate (target 0) | How often the same task runs twice by mistake | Count duplicate runs divided by total tasks | Duplicates waste resources and can cause problems | 0 |
| Queue depth under burst load | How many tasks are waiting when you send many at once | Count tasks waiting in queue during a burst | Shows if the system can handle sudden spikes | |
| Recovery time after failure injection (worker killed, broker down, db slow) | How long it takes to get back to normal after we break something on purpose | Time from failure until system works normally again | Shows how resilient the system is | |
| Cost per 1M tasks (rough estimate) | How much money it costs to run one million tasks | Calculate server and resource costs divided by task count | Helps plan budget and compare options | |

## Scheduler Simulation Metrics

Before we lean on Kafka, Postgres, and live traffic, we add a **simulation harness**: a controlled way to enqueue and dequeue jobs with fixed scenarios (tenants, weights, priorities, capacity). That lets us **validate scheduling logic in isolation**—fairness, admission, and ordering—without noise from the network, persistence, or workers. When something looks wrong, we can **replay the same inputs** and know whether the bug is in policy or in infrastructure.

For the **composed scheduler prototype** (bounded admission, weighted round robin across tenants, priority within a tenant), we want to measure whether behavior matches intent: overload is visible at the gate, tenants get roughly the share we configured, and urgent work wins inside each tenant. The table below lists counters we care about during simulation runs; they are the same ideas we will later promote to real observability.

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `enqueue_accepted_count` | How many jobs passed validation and were admitted under the current capacity limit | Confirms the system is accepting work when there is room; baseline for throughput of admitted jobs |
| `enqueue_rejected_full_count` | How many enqueue attempts failed because the total queue was at capacity | Signals overload and backpressure: clients should retry or slow down; operators watch saturation |
| `enqueue_rejected_invalid_count` | How many enqueue attempts failed because the job was invalid (for example blank ids) | Separates bad input from overload; retries will not fix invalid payloads |
| `dispatch_count_total` | How many jobs were dequeued (dispatched) in the simulation run | Validates that admitted work eventually leaves the queue under the chosen policy |
| `dispatch_count_by_tenant` | Per-tenant counts of dequeued jobs (or a breakdown by tenant id) | Shows whether weighted round robin gives each tenant the intended share of turns over time |
| `dispatch_count_by_priority` | Counts of dequeued jobs grouped by priority value | Confirms that higher-priority work is actually selected more often within each tenant when both exist |
| `queue_depth_peak` | The largest number of jobs waiting across all tenants at any point during the run | Captures worst-case backlog in the simulation; helps reason about memory and delay under load |

## Queue Wait Time and Fairness Metrics

**Queue wait time** is how long a job sits in the queue before it is dispatched. In plain terms: after a job is accepted, how long does it wait for its turn?

Wait time is often more useful than dispatch count alone. Dispatch count tells us how many jobs moved, but not whether jobs waited too long. A scheduler can dispatch many jobs and still feel unfair or slow if some jobs are stuck in line.

We track wait time in three views:

- **Overall**: tells us the general queueing health of the system.
- **By tenant**: shows fairness across customers and helps detect noisy-neighbor effects.
- **By priority**: verifies that higher-priority work actually gets faster service.

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `average_queue_wait_time` | Average wait from enqueue to dispatch across all jobs | Baseline user-facing queue delay for the whole scheduler |
| `average_queue_wait_time_by_tenant` | Average queue wait grouped by tenant id | Exposes fairness imbalances between tenants |
| `average_queue_wait_time_by_priority` | Average queue wait grouped by priority value | Confirms priority policy is producing the expected latency ordering |
| `dispatch_count_by_tenant` | Number of dispatched jobs per tenant | Useful alongside wait-time-by-tenant to understand share vs delay |
| `dispatch_count_by_priority` | Number of dispatched jobs per priority | Useful alongside wait-time-by-priority to understand urgency handling |

## Postgres Scheduling Query Metrics

KernelQ’s **first database-backed scheduling path** lives in the repository layer (`list_schedulable_jobs`, `mark_job_dispatched`). Instead of asking an in-memory queue “who is next?”, the control plane will ask **Postgres** which rows are ready and in what order. We have **not** wired production metrics or load tests for this path yet; this section names what we plan to measure so we can compare policy in simulation with **real query behavior** later.

**The schedulable query (today):**

- **Filter:** `state = 'queued'` (stored lowercase in the `jobs` table). Only jobs waiting to be picked appear; rows already `dispatched`, `running`, or in terminal states are excluded.
- **Order:** `priority DESC`, then `created_at ASC` — urgent work first; among equal priority, **older jobs first** (FIFO tie-break).

That matches the in-memory priority scheduler’s intent, but the ordering is enforced in SQL so it survives restarts and can be shared across control-plane instances.

**Future metrics (not measured yet):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `schedulable_query_latency` | Time for `list_schedulable_jobs` to return (p50 / p95 / p99) | Slow picks delay every dispatch tick; indexes and query shape must stay healthy as the table grows |
| `schedulable_query_rows_scanned` | How many rows Postgres examined to satisfy the query (from `EXPLAIN` / stats) | High scans with few results suggest a missing or mismatched index |
| `jobs_selected_per_scheduler_tick` | Count of jobs returned per call to `list_schedulable_jobs` (often tied to `LIMIT`) | Shows how much work each tick tries to hand off; useful for tuning batch size |
| `dispatch_transition_latency` | Time from “row selected” to `mark_job_dispatched` completing (queued → dispatched) | Captures claim/update cost and contention if multiple schedulers run |

**Placeholder: `EXPLAIN ANALYZE` output**

When we run the schedulable query against a realistic `jobs` table, paste the plan here (no fabricated numbers):

```
-- Example (run against local or staging Postgres when ready):
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT job_id, tenant_id, priority, state, payload,
--        retry_count, max_retries, created_at, updated_at
-- FROM jobs
-- WHERE state = 'queued'
-- ORDER BY priority DESC, created_at ASC
-- LIMIT 10;

-- TODO: paste actual EXPLAIN ANALYZE output after first benchmark run.
```

Until that run exists, treat this section as a **metrics plan**, not a report. Simulation counters above still describe in-memory prototypes; this table is the bridge toward **durable scheduling** observability once Kafka dispatch and a live dispatch loop are added.

## Scheduler Tick Metrics

KernelQ’s **scheduler tick runner** (`SchedulerTickRunner.run_once()` in the Python control plane) returns a **`SchedulerTickResult`** after each pass. Those fields are **counts you can log or export today**—no timing or broker metrics yet, but they answer “what did this tick do?” in plain numbers.

**Counts from each tick (today):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `selected_count` | How many jobs `claim_schedulable_jobs` returned this tick (at most `max_jobs_per_tick`) | Shows how much work the tick claimed in one atomic pass; sudden drops may mean an empty queue or a query problem |
| `dispatched_count` | How many selected jobs were successfully marked **`dispatched`** (`queued` → `dispatched`) | Confirms claims succeeded; this is the durable handoff count before Kafka exists |
| `skipped_count` | Jobs that were not claimed and did not error (zero with atomic `claim_schedulable_jobs` today) | With the older list-then-mark path, skips signaled races; atomic claiming moves contention into DB locking instead |
| `error_count` | Number of entries in `errors` (per-job exceptions during `mark_job_dispatched`) | Separates infrastructure failures from skips; operators can alert on non-zero errors |
| `max_jobs_per_tick` | Configured cap passed into `SchedulerTickRunner` (batch limit for the schedulable query) | Documents the tick’s intended batch size when comparing runs; tuning it trades throughput per tick vs steady load |

`error_count` is not a separate field on `SchedulerTickResult`; in practice it is **`len(result.errors)`** alongside the readable error strings the tick already collects.

**Future metrics (not measured yet):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `tick_duration` | Wall-clock time for the full `run_once()` pass (query + all mark attempts) | A slow tick delays every job waiting behind it; helps spot DB or application regressions |
| `postgres_query_latency` | Time for `list_schedulable_jobs` alone (and optionally each `mark_job_dispatched`) | Breaks out database cost inside the tick; pairs with `EXPLAIN ANALYZE` and index work above |
| `kafka_publish_latency` | Time to publish a selected job to Kafka once that step exists | Shows broker health and whether publish is the bottleneck after Postgres claims |
| `dispatch_transition_latency` | Time from selecting a row to completing `queued` → `dispatched` (or through Kafka publish later) | Captures end-to-end claim cost and contention when multiple control-plane instances run ticks |

We have **not** published numeric targets or sample dashboards for tick metrics yet. Use **`SchedulerTickResult`** in tests and logs first; add histograms and SLOs after a dispatch loop runs continuously against real Postgres (and later Kafka).

## Atomic Claiming Metrics

KernelQ’s scheduler tick now calls **`claim_schedulable_jobs`**—one Postgres transaction that locks candidate **`queued`** rows (`FOR UPDATE SKIP LOCKED`), updates them to **`dispatched`**, and returns the claimed rows. At this stage, atomic claiming matters most for **reliability** (avoiding duplicate dispatch) rather than shaving milliseconds off a hot path. Performance tuning still matters, but **correctness under multiple schedulers** is the primary win.

**Future metrics (not measured yet):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `duplicate_dispatch_rate` | How often the same `job_id` is handed off more than once (target: **0** in production) | The main reliability signal atomic claiming exists to protect; any non-zero rate means races or bugs in claim/publish logic |
| `claim_query_latency` | Time for `claim_schedulable_jobs` to complete (p50 / p95 / p99) | Slow claims delay every tick; grows with table size, index health, and contention |
| `skipped_locked_rows` | How many `queued` rows were skipped because another session held `FOR UPDATE` locks (inferred from “wanted N rows, got fewer” under load, or DB stats) | Shows multi-scheduler contention; some skips are normal, chronic starvation of low-priority work is not |
| `dispatch_transition_latency` | Time from start of claim to committed `dispatched` rows (same transaction today) | End-to-end cost of one atomic handoff; useful when comparing claim vs older list-then-mark |
| `scheduler_tick_throughput` | How many jobs claimed per second across all scheduler instances | Capacity planning once several control-plane loops run in parallel |

**Placeholder: `EXPLAIN ANALYZE` on the claim query**

Paste a real plan here after benchmarking against local or staging Postgres (no fabricated numbers):

```
-- Example (run when ready):
-- EXPLAIN (ANALYZE, BUFFERS)
-- UPDATE jobs
-- SET state = 'dispatched', updated_at = NOW()
-- WHERE job_id IN (
--     SELECT job_id
--     FROM jobs
--     WHERE state = 'queued'
--     ORDER BY priority DESC, created_at ASC
--     LIMIT 10
--     FOR UPDATE SKIP LOCKED
-- )
-- RETURNING job_id, tenant_id, priority, state, payload,
--           retry_count, max_retries, created_at, updated_at;

-- TODO: paste actual EXPLAIN ANALYZE output after first benchmark run.
```

Until that benchmark exists, treat this section as a **reliability-and-observability plan**, not a performance report.

## Load Testing Methodology

TODO: Define test scenarios, load profiles, ramp-up strategies, and success criteria.

## Failure Injection Experiments

TODO: Define failure scenarios (worker crashes, broker outages, database slowdowns) and expected recovery behaviors.
