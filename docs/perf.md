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
| `dispatched_count` | How many selected jobs were successfully marked **`dispatched`** (`queued` → `dispatched`) | Confirms Postgres claims succeeded; compare to **`published_count`** in **Scheduler Publish Metrics** when Kafka is wired |
| `skipped_count` | Jobs that were not claimed and did not error (zero with atomic `claim_schedulable_jobs` today) | With the older list-then-mark path, skips signaled races; atomic claiming moves contention into DB locking instead |
| `error_count` | Number of entries in `errors` (per-job exceptions during `mark_job_dispatched`) | Separates infrastructure failures from skips; operators can alert on non-zero errors |
| `max_jobs_per_tick` | Configured cap passed into `SchedulerTickRunner` (batch limit for the schedulable query) | Documents the tick’s intended batch size when comparing runs; tuning it trades throughput per tick vs steady load |

`error_count` is not a separate field on `SchedulerTickResult`; in practice it is **`len(result.errors)`** alongside the readable error strings the tick already collects.

**Future metrics (not measured yet):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `tick_duration` | Wall-clock time for the full `run_once()` pass (query + all mark attempts) | A slow tick delays every job waiting behind it; helps spot DB or application regressions |
| `postgres_query_latency` | Time for `list_schedulable_jobs` alone (and optionally each `mark_job_dispatched`) | Breaks out database cost inside the tick; pairs with `EXPLAIN ANALYZE` and index work above |
| `kafka_publish_latency` | Time to publish a selected job to Kafka | Shows broker health and whether publish is the bottleneck after Postgres claims; see **Scheduler Publish Metrics** |
| `dispatch_transition_latency` | Time from selecting a row to completing `queued` → `dispatched` (or through Kafka publish later) | Captures end-to-end claim cost and contention when multiple control-plane instances run ticks |

We have **not** published numeric targets or sample dashboards for tick metrics yet. Use **`SchedulerTickResult`** in tests and logs first; add histograms and SLOs after a dispatch loop runs continuously against real Postgres and Kafka.

## Scheduler Publish Metrics

Scheduler ticks can now **publish dispatch events** to Kafka after **`claim_schedulable_jobs`**. When a **`job_producer`** is configured, **`SchedulerTickResult`** adds publish-related counts you can log after each **`run_once()`**—still **counts only**, no latency histograms yet.

**Publish-related counts (today):**

| Metric | Where it lives | What it means | Why it matters |
|--------|----------------|---------------|----------------|
| `selected_count` | `SchedulerTickResult.selected_count` | How many jobs the tick **claimed** from Postgres this pass (at most `max_jobs_per_tick`) | Baseline “how much work did we pick up?” before comparing to publish outcomes |
| `dispatched_count` | `SchedulerTickResult.dispatched_count` | How many rows moved **`queued` → `dispatched`** in the claim transaction | With atomic claiming, this matches `selected_count` on success—the DB handoff count |
| `published_count` | `SchedulerTickResult.published_count` | How many **`DispatchEvent`** messages were **successfully** sent to **`kernelq.jobs.dispatch`** | Shows how many claimed jobs actually reached the broker this tick |
| `publish_error_count` | **`len(result.publish_errors)`** (not a separate field yet) | How many per-job **`publish_dispatch_event`** calls **failed** after claim | Non-zero means some jobs may be **`dispatched`** in Postgres **without** a Kafka message—see reliability gap in `docs/architecture.md` |

**Plain English check:** if `dispatched_count` is 10 but `published_count` is 8, **`publish_error_count`** should be 2—two jobs were claimed but not published.

**Why publish errors matter**

Publish failures are **silent to workers** unless you measure them. Postgres already says **`dispatched`**; Kafka never got the event, so **Go workers have nothing to consume**. Without tracking **`publish_errors`**, operators only notice missing work through stuck jobs or support tickets—not through the tick summary.

Watch for:

- **`published_count` < `dispatched_count`** — stranded handoffs (claim succeeded, publish did not)
- **Growing `publish_error_count` over time** — broker outage, misconfiguration, or network problems
- **Non-empty `publish_errors` strings** — each entry names a `job_id` and exception type for debugging

Today the tick **does not roll back** Postgres on publish failure. Metrics help you **detect** the gap until an **outbox** or **retryable dispatch** mechanism fixes it.

**Future metrics (not measured yet):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `kafka_publish_latency` | Time from start of `publish_dispatch_event` to broker ack (p50 / p95 / p99) | Slow publish delays every claimed job in the tick; separates app time from broker slowness |
| `broker_error_rate` | Share of publish attempts that fail with broker or client errors | Early warning for Kafka health, auth, or quota issues |
| `dispatch_to_consume_latency` | Time from successful publish until a worker consumes the message | End-to-end handoff latency—scheduler did its job, but is execution keeping up? |
| `producer_retry_count` | How often the producer retries after transient failures | Shows whether failures are flaky vs sustained; pairs with outbox/retry design later |

We have **not** defined numeric SLOs or dashboard panels for these yet. Log **`SchedulerTickResult`** fields in tests and manual runs first; add timers and histograms when the tick loop runs continuously against real Kafka.

**Interview sound bite:** *“Log selected, dispatched, published, and len(publish_errors)—if dispatched beats published, jobs are stranded until outbox/retry; latency and broker error rate come next.”*

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

## Postgres EXPLAIN for Scheduler Queries

Every scheduler tick hits Postgres to find and claim **`queued`** work. As the **`jobs`** table grows, that **queued-job claim / scheduling query** must stay cheap and predictable. **`EXPLAIN`** is how we inspect the database plan **before** production load—not to guess, but to see whether Postgres uses indexes or scans the whole table.

**Why we inspect scheduler queries:**

- Ticks run **often**; a slow plan hurts every waiting job.
- The query has a fixed shape: filter by **`state`**, sort by **`priority`** and **`created_at`**, **`LIMIT`** a batch, and (for claims) **`FOR UPDATE SKIP LOCKED`**.
- Wrong plans show up only at scale—local dev with ten rows can hide a sequential scan on millions of rows.

**What `EXPLAIN` shows:**

- The **planned steps** Postgres would take (scan types, index names, joins, sorts, limits).
- **Estimated** row counts and costs—not real execution time.

**What `EXPLAIN ANALYZE` does:**

- Runs the query for real, then prints the plan **plus actual** row counts and **execution time** per step.
- **Important:** it **executes** the statement. Read-only `SELECT` plans are safer to try on dev; an `UPDATE` claim query will mutate rows—use staging or sample data.

**What to watch for in the plan:**

| Signal | What it means | Why it matters |
|--------|---------------|----------------|
| **Index usage** | Steps like `Index Scan` or `Bitmap Index Scan` on `state` / `(state, priority)` | Good sign Postgres is not reading every row in `jobs` |
| **Sequential scans** | `Seq Scan on jobs` for the schedulable filter | Often bad at large table size; may mean a missing or unused index |
| **Sorting** | `Sort` with high cost or many rows | Sorting a huge `queued` set before `LIMIT` is expensive; ideal plans narrow rows first |
| **Estimated rows** | Planner guesses vs reality (compare in `EXPLAIN ANALYZE`) | Bad estimates can pick the wrong plan; stale statistics are a common cause |
| **Execution time** | Actual milliseconds on each node (`EXPLAIN ANALYZE` only) | What operators feel per tick; track p95/p99 later, not just one manual run |

**How to Run**

From the repository root, with local Postgres up (`docker compose up -d postgres`) and migrations applied:

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/explain_claim_schedulable_jobs.sql
```

That script (`control_plane/sql/explain_claim_schedulable_jobs.sql`) includes optional sample rows, **`EXPLAIN`** on the schedulable **`SELECT`**, **`EXPLAIN ANALYZE`** on the same query (with a comment that it runs), and **`EXPLAIN`** on the **`FOR UPDATE SKIP LOCKED`** claim subquery.

**Observed Plan Notes**

Paste real output here after you run the script (no invented numbers):

```
-- TODO: paste EXPLAIN / EXPLAIN ANALYZE output from explain_claim_schedulable_jobs.sql
-- Note: KernelQ stores state as lowercase 'queued' in Postgres.
-- Compare: index scan vs seq scan, sort cost, estimated vs actual rows, Execution Time.
```

**Interview sound bite:** *“We EXPLAIN the queued-job claim query to confirm index-backed plans; EXPLAIN ANALYZE runs it and shows real time—watch for seq scans and sorts on large row counts before the LIMIT.”*

## Scheduler Query Indexing

On **Day 23**, we ran `explain_claim_schedulable_jobs.sql` against local Postgres with a handful of sample rows. The plan showed **`Seq Scan on jobs`** (filter `state = 'queued'`), then **`Sort`** on `priority DESC, created_at`, then **`Limit`**. **`EXPLAIN ANALYZE`** on the same shape reported a small in-memory sort and sub-millisecond execution on that tiny dataset.

**That is okay on small tables:** when `jobs` has very few rows, Postgres often chooses a sequential scan because reading the whole table is cheaper than index setup. The planner is optimizing for **current size**, not a million-row future.

**It is not enough for scale:** as `jobs` grows and most rows are **not** `queued` (terminal states, history, in-flight work), a seq scan plus sort on every tick means reading far more data than the scheduler needs. Latency per tick rises; queued work waits longer. We add an index that **matches the query shape** so the planner can narrow to waiting work in dispatch order.

**Migration 002 index:**

```sql
CREATE INDEX IF NOT EXISTS idx_jobs_state_priority_created_at
    ON jobs (state, priority DESC, created_at ASC);
```

**How this matches the scheduler query:**

```sql
WHERE state = 'queued'
ORDER BY priority DESC, created_at ASC
LIMIT <n>
```

- **`state`** (first column) — same filter as `WHERE state = 'queued'`.
- **`priority DESC`** — same urgency ordering (higher priority first).
- **`created_at ASC`** — same FIFO tie-break among equal priority.

The atomic claim path (`claim_schedulable_jobs`) uses the same ordering inside its `FOR UPDATE SKIP LOCKED` subquery, so this index supports both **read** (`list_schedulable_jobs`) and **claim** plans.

**Why column order and direction matter:** composite indexes are most helpful when leading columns match **`WHERE`** clauses and sort direction matches **`ORDER BY`**. Putting `state` first targets the schedulable subset; `DESC` / `ASC` on the next columns align with how KernelQ picks the next batch without an extra sort step when the planner uses the index.

**Before/After EXPLAIN Notes**

Run `explain_claim_schedulable_jobs.sql` **before** and **after** applying `control_plane/migrations/002_add_scheduler_claim_index.sql`. Paste whether the plan changed (no fabricated numbers):

```
-- BEFORE migration 002 (Day 23 baseline on sample data):
--   Seq Scan on jobs → Sort → Limit
--   (observed locally; acceptable on tiny tables)

-- AFTER migration 002:
-- TODO: paste EXPLAIN / EXPLAIN ANALYZE output and note:
--   - Index Scan / Bitmap Index Scan on idx_jobs_state_priority_created_at?
--   - Seq Scan still chosen (small table — planner may keep seq scan)?
--   - Sort step reduced or removed?
--   - Execution Time (EXPLAIN ANALYZE only — paste real value)
```

**Interview sound bite:** *“Day 23 seq scan was fine on a dev-sized table; at scale we added `(state, priority DESC, created_at ASC)` so the claim query’s filter and ORDER BY match the index—then we EXPLAIN before and after to prove the plan improved.”*

## Large Dataset Query Plan Experiment

**Small tables may favor Seq Scan:** with only a few rows in `jobs`, Postgres often reads the whole table in one pass—it is simple and fast enough that an index never pays off.

**Larger tables may favor indexes:** when thousands of rows exist and only a **fraction** are `queued`, scanning everything each tick wastes work. The planner may then choose **`Index Scan`** (or similar) on **`idx_jobs_state_priority_created_at`** so it can filter and walk rows in dispatch order. **You cannot assume that from docs alone**—run **`EXPLAIN`** on your data and read the plan.

**Local synthetic data:** KernelQ includes **`control_plane/sql/seed_large_jobs_dataset.sql`**, which inserts about **5000** synthetic jobs (`seed-tenant-*` / `seed-job-*`) with mixed states (mostly **`queued`**, plus some **`dispatched`**, **`succeeded`**, **`failed`**), varied **priority**, **tenant_id**, and **created_at**. That is **local benchmarking data only**—not for production.

### Experiment Workflow

From the repository root, with Postgres up and migrations **001** and **002** applied:

**1. Seed the large dataset**

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/seed_large_jobs_dataset.sql
```

**2. Rerun EXPLAIN (includes `SELECT COUNT(*)` and index list)**

```bash
docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/sql/explain_claim_schedulable_jobs.sql
```

Compare output to a **before-seed** run on a small table. Also compare **before vs after migration 002** if you have both captures.

**Observed results (paste after you run — no invented numbers):**

| Field | Notes |
|-------|--------|
| **Table size** | `jobs_row_count` from `SELECT COUNT(*) FROM jobs` (TODO: paste) |
| **Observed planner choice** | e.g. Seq Scan, Index Scan, Bitmap Index Scan (TODO: paste from EXPLAIN) |
| **Index scan appeared?** | Did the plan reference `idx_jobs_state_priority_created_at`? (TODO: yes / no) |
| **Execution timing notes** | From `EXPLAIN ANALYZE` only — Execution Time, sort steps (TODO: paste) |

```
-- TODO: paste EXPLAIN / EXPLAIN ANALYZE snippets from after seed_large_jobs_dataset.sql
-- Example prompts (fill in with real output):
--   jobs_row_count: ___
--   Plan root node: ___
--   Uses idx_jobs_state_priority_created_at: yes / no
--   Execution Time: ___ ms
```

**Interview sound bite:** *“We seeded ~5k jobs locally, reran EXPLAIN, and compared plans—small tables seq-scan; at scale the planner may pick our composite index if statistics say it is cheaper.”*

## Worker Message Error Metrics

The Go worker’s poll loop (`KafkaConsumer.Run` in `worker/internal/worker/kafka_consumer.go`) already maintains **`ConsumerStats`** counters. **`cmd/consumer`** prints them on clean shutdown. We have **not** exported them to Prometheus or dashboards yet—this section names the metrics we plan to expose.

**Counters today (in-process):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `messages_seen` | Every **`*kafka.Message`** received from the poll loop (before success/failure) | Baseline volume—how much traffic the worker touched |
| `messages_processed` | Messages that passed parse, validation, and handler/executor without error | Healthy throughput on **`kernelq.jobs.dispatch`** |
| `message_errors` | Messages where **`ProcessKafkaMessage`** failed (bad JSON, validation, handler error) | Poison or drift traffic; worker keeps polling but work was not executed |
| `kafka_errors` | Broker **`kafka.Error`** events that stopped **`Run`** | Infra/client problems—different from bad payloads |

**Derived rate (future):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `invalid_message_rate` | **`message_errors / messages_seen`** (when `messages_seen > 0`) | Single number for “how much of our intake is garbage?” |

**How to read `invalid_message_rate`:**

- It is the share of seen records that failed processing—not the same as end-to-end job failure rate.
- In **healthy operation**, this rate should stay **near 0**. Any sustained increase usually means **producer bugs**, **schema drift**, **stale test data on the topic**, or a **poison message** that will never parse.
- We have **not** set numeric SLO thresholds yet; treat “near 0” as an operational goal, not a measured target in this doc.

**Today vs later:**

- **Today:** stats live **in-process** on **`KafkaConsumer.Stats`** and appear in logs when the consumer stops cleanly.
- **Later:** promote the same names to **Prometheus** (or similar) counters/gauges, add histograms for processing latency, and alert when **`invalid_message_rate`** or **`kafka_errors`** rise.

**Future metrics (not measured yet):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `processing_latency` | Time per message from poll to handler/executor completion (p50 / p95 / p99) | Shows whether execution is keeping up with publish rate |
| `shutdown_count` | How many clean shutdowns (SIGINT/SIGTERM) vs crash exits | Separates operator stops from fatal broker failures |

**Interview sound bite:** *“Log messages_seen, messages_processed, message_errors, and invalid_message_rate = errors/seen—should be near zero when healthy; Prometheus and DLQ come later.”*

## DLQ Metrics Planned

When workers route failures through **`DeadLetterProducer`**, we track DLQ outcomes alongside **Worker Message Error Metrics**. **No numeric results, dashboards, or SLOs exist yet** for production.

**In-process today (`ConsumerStats` in `kafka_consumer.go`):**

| Stat | What it means | Why it matters |
|------|---------------|----------------|
| `dead_letters_published` | **`DeadLetterEvent`** successfully passed to **`PublishDeadLetter`** | Confirms routing ran; pairs with **`message_errors`** |
| `dead_letter_publish_errors` | DLQ publish attempts that **failed** (producer error, validation, future broker failure) | **Target should be 0** in healthy operation—non-zero means bad messages may lack a durable DLQ record |

These counters are printed from tests and available on shutdown when **`cmd/consumer`** wires a producer and exports stats. **Later** they become **Prometheus counters** (names may align with `dlq_publish_count` / `dlq_publish_error_count` below).

**Future metrics (not measured in production yet):**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `dlq_publish_count` | Prometheus name for successful DLQ publishes (maps to **`dead_letters_published`**) | Confirms poison traffic is leaving the dispatch topic with evidence preserved |
| `dlq_publish_error_count` | Prometheus name for failed DLQ publishes (maps to **`dead_letter_publish_errors`**) | Alert when we cannot record failures—**target 0** |
| `invalid_message_rate` | **`message_errors / messages_seen`** (same derived rate as above) | Early signal of **schema drift** or **bad producers** before DLQ depth grows |
| `poison_message_count` | Messages classified as permanently bad (eligible for DLQ, not worth retry on dispatch) | Tracks volume of “will never succeed as-is” records; pairs with DLQ publish counters |

**Why DLQ metrics matter:**

- Rising **`invalid_message_rate`** with low **`dead_letters_published`** suggests failures are counted but not durably recorded (no producer wired, or publish failures).
- Rising **`dead_letters_published`** points to **producer bugs**, **manual test traffic**, or **contract drift**—inspect **`reason`** and **`original_value`** on DLQ messages.
- **`dead_letter_publish_errors`** should stay **0**; any sustained increase means “message was bad” but “we failed to record it on DLQ.”

We have **not** measured these in production or defined numeric thresholds yet.

## Worker Result Publishing Metrics Planned

**`DispatchEventHandler`** now calls **`PublishResult`** after **`Executor.Execute`** when **`ResultProducer`** is wired. We will track result handoff alongside dispatch and DLQ metrics. **No numeric measurements, dashboards, or SLOs exist yet.**

| Metric | What it means | Why it matters |
|--------|---------------|----------------|
| `handler_result_publish_success_total` | **`WorkerResultEvent`** successfully published from the handler to **`kernelq.jobs.results`** | Confirms execution outcomes reached the control-plane feedback lane |
| `handler_result_publish_error_total` | Handler **`PublishResult`** failures (validate, JSON, produce, flush) | **Target should be 0**—non-zero means Postgres may never learn the job outcome |
| `execution_to_result_publish_latency` | Time from **`Execute`** completion to successful **`PublishResult`** | Surfaces delay or stalls between outcome and broker handoff |
| `result_publish_latency` | Time inside **`PublishResult`** (produce + flush) | Kafka producer slowness isolated from executor time |
| `dispatch_to_result_latency` | Time from dispatch consume to result publish | End-to-end worker turnaround |

Prometheus names may align with **`handler_result_publish_success_total`** / **`handler_result_publish_error_total`** above; counters and histograms land when **`cmd/consumer`** exports stats from the handler path.

## Load Testing Methodology

TODO: Define test scenarios, load profiles, ramp-up strategies, and success criteria.

## Failure Injection Experiments

TODO: Define failure scenarios (worker crashes, broker outages, database slowdowns) and expected recovery behaviors.
