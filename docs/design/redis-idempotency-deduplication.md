# Redis Idempotency and Deduplication Design

Design for fast duplicate suppression across KernelQ’s Kafka handoffs. **Day 96** adds local Redis infrastructure (`docker-compose.yml`); **dedupe logic is not implemented in this milestone.**

**Interview sound bite:** *“Postgres owns job state; Redis owns short-lived ‘have we seen this event?’ keys with TTLs; Kafka stays at-least-once.”*

---

## 1. Summary

KernelQ will use **Redis** as a **fast idempotency and duplicate-suppression boundary** between:

- the **Python control plane** (scheduler publish, result consumer), and  
- the **Go worker plane** (dispatch intake, execution, result publish).

Redis answers **“have we already processed this logical event?”** in sub-millisecond time. **Postgres remains the system of record** for durable job lifecycle (`queued`, `dispatched`, `succeeded`, `retry_scheduled`, `dead_lettered`, …).

Redis is **not** a second database for jobs—it is a **TTL-backed dedupe cache** ahead of expensive or irreversible work.

---

## 2. Problem

KernelQ’s messaging path is **at-least-once** by design:

| Source | Replay / duplicate risk |
|--------|-------------------------|
| **Kafka** | Consumer rebalance, crash before offset commit, producer retries → same dispatch or result message delivered again |
| **Workers** | Crash after `Execute` but before `PublishResult` → redispatch may re-run; duplicate result events may land on `kernelq.jobs.results` |
| **Control plane** | Scheduler tick retries, publish retries after Postgres claim → duplicate dispatch events possible (see `publish_errors` gap today) |
| **Result consumer** | `poll_once` loop (future daemon) may re-read results; stale topic history during benchmarks/smoke tests |

Without dedupe:

- The same **`job_id`** may be **executed twice** (wasted CPU, unsafe side effects).
- The same **result event** may **update Postgres twice** (incorrect metrics, retry confusion).
- Operators cannot trust throughput benchmarks under failure injection.

**Atomic Postgres claiming** (Day 22) reduces duplicate *scheduling* from the DB side; it does **not** replace Kafka replay protection on workers or result consumers.

---

## 3. Proposed Keys

Keys are **namespaced**, include stable **idempotency dimensions** (`job_id`, `attempt`, or `event_id`), and use a consistent prefix:

| Key pattern | Purpose | Example |
|-------------|---------|---------|
| `kernelq:dispatch:<job_id>:<attempt>` | Scheduler or producer already published this dispatch handoff | `kernelq:dispatch:job-abc:0` |
| `kernelq:execution:<job_id>:<attempt>` | Worker already accepted / started this execution unit | `kernelq:execution:job-abc:0` |
| `kernelq:worker-result:<job_id>:<attempt>` | Result consumer already applied this worker outcome | `kernelq:worker-result:job-abc:0` |
| `kernelq:dedupe:<event_id>` | Generic idempotency for opaque event ids (audit, future outbox) | `kernelq:dedupe:evt-7f3a…` |

**`<attempt>`** aligns with `retry_count` / retry generation in Postgres when retries are in play. For first-run jobs, **`0`** is the default attempt dimension.

**Value payload (minimal):** optional JSON `{"seen_at": "<iso>", "worker": "…"}` or a sentinel `1`—dedupe is keyed by **existence**, not value semantics.

---

## 4. TTL Strategy

Redis keys **must expire** to avoid unbounded memory growth.

| Principle | Detail |
|-----------|--------|
| **TTL > max replay window** | Key lives at least as long as Kafka retention + retry backoff + operator requeue window you intend to protect |
| **TTL tied to retry policy** | If `retry_after` can be hours, dispatch/execution TTL must cover that window or duplicates after expiry are accepted risk |
| **Shorter TTL for hot paths** | Ephemeral burst dedupe (e.g. 15–60 minutes) for dev; production tuned from retention SLOs |
| **No TTL = bug** | Every `SET` in the dedupe path specifies `EX` or `PX` |

**Starting point (local dev, subject to change):**

- Dispatch / execution / result keys: **24h** default TTL, documented in config.
- Generic `kernelq:dedupe:<event_id>`: match **Kafka topic retention** or job max lifetime, whichever is larger.

When TTL expires, a **duplicate is possible again**—Postgres state checks remain the backstop (e.g. ignore `succeeded` → `succeeded`).

---

## 5. Semantics

### First-seen wins (`SET NX`)

```
SET kernelq:execution:job-abc:0 1 NX EX <ttl>
```

| Result | Meaning | Action |
|--------|---------|--------|
| **OK** (key set) | First time seeing this logical event | Proceed: execute, publish, or update Postgres |
| **nil** (key exists) | Duplicate / replay | **Skip** side effect; increment `dedupe_hits` metric; optionally return prior outcome |
| **Error** | Redis down | See **Failure modes** — policy TBD (fail closed vs degrade) |

### Postgres vs Redis roles

| Store | Role |
|-------|------|
| **Postgres** | Authoritative job rows, state machine, retry counters, audit |
| **Redis** | Fast “already handled this idempotency key?” cache |

A successful path **writes Postgres** for durable outcome and **sets Redis** to suppress near-term duplicates. On Redis miss after TTL, Postgres **`state`** and **`retry_count`** still decide correctness.

### Dispatch vs result dedupe (different boundaries)

| Boundary | What duplicate means | Idempotency key |
|----------|----------------------|-----------------|
| **Dispatch dedupe** | Same job dispatch consumed twice on `kernelq.jobs.dispatch` | `kernelq:dispatch:` / `kernelq:execution:` |
| **Result dedupe** | Same `WorkerResultEvent` applied twice in control plane | `kernelq:worker-result:` |

Both are required; either alone leaves a gap in the pipeline.

---

## 6. Failure Modes

| Failure | Symptom | Mitigation direction |
|---------|---------|----------------------|
| **Redis unavailable** | Every `SET NX` fails | **Fail closed** for execution (skip or DLQ) vs **fail open** (log + rely on Postgres)—document per path; alert on error rate |
| **TTL too short** | Key expires; replay processed again | Increase TTL; align with retry retention; monitor `dedupe_expired_replays` |
| **Redis state lost** | Restart/flushed instance | Treat as cache miss; Postgres + idempotent SQL contain damage; rebuild keys on next success optional |
| **Duplicate after TTL expiry** | Rare double execution / double state write | Terminal states in Postgres should be idempotent updates; metrics on unexpected transitions |
| **Mismatch with Postgres** | Redis says “seen” but row missing or still `queued` | Prefer Postgres on conflict; delete stale Redis key; reconciliation job (future) |
| **Clock / TTL skew** | Early expiry across nodes | Use relative `EX` from set time; single Redis primary in dev |

**Today:** no Redis client in app code—local **`docker compose up -d redis`** only.

---

## 7. Implementation Plan

| Day | Scope |
|-----|--------|
| **96** | Redis in `docker-compose.yml`; this design doc; docs/runbooks mention Redis |
| **97** | Redis client wrapper + config (`REDIS_URL`, connection health) |
| **98** | In-memory fake idempotency store + unit tests (no broker) |
| **99** | Redis-backed store implementing same interface |
| **100+** | Integrate dispatch dedupe (worker intake), result dedupe (Python consumer), metrics |

Integration order: **interface → fake → Redis → worker → result consumer**—same pattern as Kafka pause/resume (policy before adapter).

---

## 8. Non-Goals

- **Not replacing Postgres** — job lifecycle and audit stay in `jobs` table.
- **Not replacing Kafka offsets** — consumer groups still commit offsets; Redis is additive replay protection.
- **Not implementing dedupe logic on Day 96** — infrastructure and design only.
- **Not exactly-once Kafka** — goal is **effective-once** behavior for job side effects.
- **Not API idempotency keys for HTTP enqueue yet** — future `Idempotency-Key` header can reuse `kernelq:dedupe:<event_id>` pattern.

---

## Related

- [ADR-0002 Kafka choice](../decisions/ADR-0002-kafka-choice.md) — at-least-once handoff
- [kafka-pause-resume-backpressure.md](kafka-pause-resume-backpressure.md) — worker intake backpressure (orthogonal)
- [day90 checkpoint](../checkpoints/day90.md) — Redis on production-readiness roadmap
- [architecture.md](../architecture.md) — control plane vs worker split
