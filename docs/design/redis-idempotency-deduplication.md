# Redis Idempotency and Deduplication Design

Design for fast duplicate suppression across KernelQ’s Kafka handoffs. **Day 96–101:** store, keys, consumer dedupe, Redis smoke. **Day 102:** env-driven backend — **`KERNELQ_IDEMPOTENCY_BACKEND=memory|redis`** (memory default); Redis via redis-cli wrapper (no Python Redis package); **`consume_result_once.py`** wired. Redis unavailable → fail clear. Dispatch/execution dedupe still future.

**Interview sound bite:** *“Postgres owns job state; Redis owns short-lived ‘have we seen this event?’ keys with TTLs; Kafka stays at-least-once.”*

---

## 1. Summary

KernelQ will use **Redis** as a **fast idempotency and duplicate-suppression boundary** between:

- the **Python control plane** (scheduler publish, result consumer), and  
- the **Go worker plane** (dispatch intake, execution, result publish).

Redis answers **“have we already processed this logical event?”** in sub-millisecond time. **Postgres remains the system of record** for durable job lifecycle (`queued`, `dispatched`, `succeeded`, `retry_scheduled`, `dead_lettered`, …).

Redis is **not** a second database for jobs—it is a **TTL-backed dedupe cache** ahead of expensive or irreversible work.

**Day 97:** **`IdempotencyStore.try_claim(key, ttl_seconds) -> bool`** — `True` = first claimant, `False` = duplicate. **`InMemoryIdempotencyStore`** for unit tests; handlers depend on the interface, not Redis directly.

**Day 98:** **`RedisIdempotencyStore`** — same contract via duck-typed ``client.set(..., nx=True, ex=ttl)``; no ``redis`` import in the store module.

**Day 99:** **`kernelq/idempotency_keys.py`** — stdlib helpers that build the logical key segment passed to **`try_claim`**. **`RedisIdempotencyStore`** prepends its namespace (default **`kernelq:idempotency`**). Handlers must call the helpers instead of ad-hoc string formatting so dispatch, execution, result, and event paths stay aligned across components.

**Day 100:** **`ResultConsumerRunner`** (`result_consumer.py`) claims **`worker_result_key(job_id, attempt)`** before **`ResultStateHandler`**. Duplicate worker results are **skipped** (handler not called). Observability: **`duplicate_messages`** counter and **`event=duplicate_worker_result job_id=… attempt=…`** log line. Default store is **`InMemoryIdempotencyStore`**; production wiring can inject **`RedisIdempotencyStore`**. Store errors propagate (fail fast).

**Day 101:** **`scripts/smoke_result_idempotency_redis.py`** — two simulated **`try_claim`** calls for the same **`(job_id, attempt)`** against live Redis; confirms **`first_claim=true`**, **`second_claim=false`**. Store/key path only — not a full Kafka replay or Postgres handler smoke.

**Day 102:** **`idempotency_config.py`** — **`build_idempotency_store_from_env()`**. **`KERNELQ_IDEMPOTENCY_BACKEND`**: missing/`memory` → **`InMemoryIdempotencyStore`**; `redis` → **`RedisIdempotencyStore`** + **`RedisCliClient`** (`redis-cli -h … -p … SET … NX EX …`). **`KERNELQ_REDIS_HOST`** (default `localhost`), **`KERNELQ_REDIS_PORT`** (`6379`), **`KERNELQ_REDIS_NAMESPACE`** (`kernelq:idempotency`). **`consume_result_once.py`** uses the configured store and logs **`idempotency_backend=memory|redis`**. Redis errors propagate (fail fast).

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

Keys include stable **idempotency dimensions** (`job_id`, `attempt`, or `event_id`) and a **stage-specific prefix**. **Day 99** centralizes the logical segment in **`idempotency_keys.py`** — callers pass the return value to **`IdempotencyStore.try_claim`**; **`RedisIdempotencyStore`** stores it as **`kernelq:idempotency:<logical_key>`** (default namespace).

| Helper | Logical key (→ `try_claim`) | Purpose | Example |
|--------|----------------------------|---------|---------|
| **`dispatch_key(job_id, attempt)`** | `dispatch:<job_id>:<attempt>` | Scheduler or producer already published this dispatch handoff | `dispatch:job-abc:0` |
| **`execution_key(job_id, attempt)`** | `execution:<job_id>:<attempt>` | Worker already accepted / started this execution unit | `execution:job-abc:0` |
| **`worker_result_key(job_id, attempt)`** | `worker-result:<job_id>:<attempt>` | Result consumer already applied this worker outcome | `worker-result:job-abc:0` |
| **`event_key(event_id)`** | `event:<event_id>` | Generic idempotency for opaque event ids (audit, future outbox) | `event:evt-7f3a…` |

**Do not** hand-roll these strings in handlers — use the helpers so Python control plane, Go worker, and tests cannot drift apart.

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
- Generic **`event_key(event_id)`** keys: match **Kafka topic retention** or job max lifetime, whichever is larger.

When TTL expires, a **duplicate is possible again**—Postgres state checks remain the backstop (e.g. ignore `succeeded` → `succeeded`).

---

## 5. Semantics

### First-seen wins (`SET NX`)

Python contract (Day 97): **`IdempotencyStore.try_claim`** — same semantics as:

```
SET kernelq:idempotency:execution:job-abc:0 1 NX EX <ttl>
```

(Logical key from **`execution_key("job-abc", 0)`**; **`RedisIdempotencyStore`** adds the **`kernelq:idempotency:`** namespace.)

| Result | Meaning | Action |
|--------|---------|--------|
| **OK** / `try_claim` → **`True`** | First time seeing this logical event | Proceed: execute, publish, or update Postgres |
| **nil** / `try_claim` → **`False`** | Duplicate / replay | **Skip** side effect; increment `dedupe_hits` metric; optionally return prior outcome |
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
| **Dispatch dedupe** | Same job dispatch consumed twice on `kernelq.jobs.dispatch` | **`dispatch_key`** / **`execution_key`** |
| **Result dedupe** | Same `WorkerResultEvent` applied twice in control plane | **`worker_result_key`** |

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

**Today:** **Day 102** env-driven idempotency backend; **Day 101** Redis smoke. Dispatch/execution dedupe not yet. Local Redis: **`docker compose up -d redis`**.

---

## 7. Implementation Plan

| Day | Scope |
|-----|--------|
| **96** | Redis in `docker-compose.yml`; this design doc; docs/runbooks mention Redis |
| **97** | **`IdempotencyStore`** + **`InMemoryIdempotencyStore`**; unit tests |
| **98** | **`RedisIdempotencyStore`** (duck-typed client); fake-client tests; optional redis-cli smoke |
| **99** | **`idempotency_keys.py`** — **`worker_result_key`**, **`dispatch_key`**, **`execution_key`**, **`event_key`**; unit tests; prevents key drift between components |
| **100** | **`ResultConsumerRunner`** — **`worker_result_key`** + **`try_claim`**; skip duplicates; **`duplicate_messages`** / **`event=duplicate_worker_result`**; default in-memory store |
| **101** | **`smoke_result_idempotency_redis.py`** — live **`worker_result_key`** + Redis **`SET NX EX`** smoke (redis-cli; no Python Redis package) |
| **102** | **`idempotency_config.py`** — **`KERNELQ_IDEMPOTENCY_BACKEND`**; **`consume_result_once.py`** uses configured store |
| **103+** | Worker **execution** + scheduler **dispatch** dedupe; full Kafka replay smoke |

Integration order: **interface → in-memory tests → Redis adapter → canonical keys → result consumer → worker/dispatch handlers**.

---

## 8. Non-Goals

- **Not replacing Postgres** — job lifecycle and audit stay in `jobs` table.
- **Not replacing Kafka offsets** — consumer groups still commit offsets; Redis is additive replay protection.
- **Not full Kafka replay smoke yet** — Day 101 validates store + key path only; end-to-end duplicate result on **`kernelq.jobs.results`** remains future work.
- **Not dispatch/execution dedupe yet** — Go worker intake and scheduler publish unchanged.
- **Not exactly-once Kafka** — goal is **effective-once** behavior for job side effects.
- **Not API idempotency keys for HTTP enqueue yet** — future `Idempotency-Key` header can reuse **`event_key(event_id)`**.

---

## Related

- [ADR-0002 Kafka choice](../decisions/ADR-0002-kafka-choice.md) — at-least-once handoff
- [kafka-pause-resume-backpressure.md](kafka-pause-resume-backpressure.md) — worker intake backpressure (orthogonal)
- [day90 checkpoint](../checkpoints/day90.md) — Redis on production-readiness roadmap
- [architecture.md](../architecture.md) — control plane vs worker split
