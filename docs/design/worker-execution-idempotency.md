# Worker Execution Idempotency Design

Design for **duplicate execution suppression** on the Go worker plane when **`kernelq.jobs.dispatch`** messages replay (Kafka rebalance, offset rewind, consumer restart, producer retry).

**Interview sound bite:** *“Dispatch dedupe stops double publish; execution dedupe stops double run; result dedupe stops double Postgres writes—Postgres still owns job state.”*

---

## 1. Summary

Worker execution dedupe prevents **running the same logical job attempt twice** when a dispatch message is delivered again. Before **`Executor.Execute`**, the worker claims **`execution_key(job_id, attempt)`** in Redis (or an in-memory store in tests). **First claim → execute.** **Duplicate claim → skip execution** and log or emit a skipped/duplicate outcome—not a failure.

This closes the gap between **scheduler dispatch dedupe** (Python publish) and **result consumer dedupe** (Python Postgres updates). Kafka remains **at-least-once**; side effects become **effectively-once** per **`(job_id, attempt)`**.

---

## 2. Current State

| Layer | Status |
|-------|--------|
| **Dispatch dedupe** | **Integrated** — **`SchedulerTickRunner`** claims **`dispatch_key(job_id, retry_count)`** before Kafka publish (**Day 108**) |
| **Result dedupe** | **Integrated** — **`ResultConsumerRunner`** claims **`worker_result_key(job_id, attempt)`** before Postgres updates (**Day 100+**) |
| **Canonical key** | **`execution_key(job_id, attempt)`** → **`execution:<job_id>:<attempt>`** exists in **`control_plane/kernelq/idempotency_keys.py`** (**Day 99**) |
| **Go worker** | **Day 111:** **`RedisIdempotencyStore`** (go-redis/v9 `SetNX`); **handler not wired yet** |

Dispatch dedupe alone does not protect against replay **after** a message reaches the worker (offset reset, second consumer, crash before commit). Result dedupe alone does not prevent **duplicate side effects** inside the executor (API calls, file writes, billing). Execution dedupe sits at the **worker intake boundary**.

---

## 3. Proposed Behavior

```
Kafka dispatch message
  → parse / validate DispatchEvent
  → attempt := retry_count (or explicit attempt field when added)
  → key := execution:<job_id>:<attempt>
  → try_claim(key, ttl_seconds)
       ├─ True  → Executor.Execute → PublishResult (normal path)
       └─ False → skip Execute; log event=duplicate_execution (or emit skipped result)
```

**Duplicate path:** do **not** treat as handler error or DLQ. Increment a **`duplicate_executions`** counter (future). Optionally publish a **skipped/duplicate result** so the control plane can observe the skip—exact shape TBD (must not double-apply Postgres state; result consumer dedupe is the backstop).

**Alignment with Python keys:** Go must build the same logical segment as **`execution_key(job_id, attempt)`** so Redis namespace + key match the control-plane helpers.

---

## 4. Semantics

| Topic | Rule |
|-------|------|
| **Redis operation** | **`SET key value NX EX ttl`** — same spirit as Python **`IdempotencyStore.try_claim`** |
| **TTL** | Based on **retry retention** (e.g. 24h default, aligned with dispatch/result dedupe); must cover max time a replay could arrive before retry generation advances |
| **Postgres** | **Source of truth** for **`jobs.state`**; Redis is a TTL cache only |
| **Duplicate skip** | **Not a failure** — expected under at-least-once delivery; do not increment error/DLQ counters for healthy skips |
| **Attempt** | Same **`(job_id, attempt)`** as dispatch and result paths; **`attempt`** maps to **`retry_count`** on first integration |

---

## 5. Failure Modes

| Failure | Symptom | Mitigation direction |
|---------|---------|----------------------|
| **Redis unavailable** | **`try_claim`** errors | **Fail closed** (skip execute, alert) vs **fail open** (log + execute)—document per environment; prefer fail closed for non-idempotent work |
| **TTL too short** | Key expires; replay executes again | Increase TTL; align with retry window; metric on unexpected re-execution |
| **Worker crash after claim, before result** | Key live, result may never publish | Retry dispatch may hit duplicate execution skip; rely on redispatch + result path; reconciliation future |
| **Duplicate after TTL expiry** | Rare double execution | Terminal Postgres updates should be idempotent; alert on duplicate execution rate |
| **Redis vs Postgres mismatch** | Redis “seen” but job still **`queued`** | Prefer Postgres; optional key delete on reconciliation; do not override durable state from cache alone |

---

## 6. Implementation Plan

| Step | Scope |
|------|--------|
| **1** | ✅ Go **`IdempotencyStore`** — **`TryClaim(key, ttl) (bool, error)`** (**Day 110**) |
| **2** | ✅ **`InMemoryIdempotencyStore`** + unit tests (no Redis/Kafka) (**Day 110**) |
| **3** | ✅ Redis adapter — **`RedisIdempotencyStore`** / go-redis **`SetNX`** (**Day 111**) |
| **4** | Wire into **`DispatchEventHandler`** (or **`ConsumerRunner`**) before **`Execute`** |
| **5** | Smoke test — replay same dispatch twice; second run skips execute |
| **6** | Metrics/logs — **`duplicate_executions`**, **`event=duplicate_execution`**; optional Prometheus (future) |

**Dependency order:** interface → in-memory tests → Redis → handler → smoke.

---

## 7. Non-Goals (today)

- **Not wired into the handler today** — interface, in-memory, and Redis stores exist (**Day 110–111**); handler integration still future
- **Not replacing dispatch dedupe** — scheduler publish boundary stays separate (**`dispatch_key`**)
- **Not replacing result dedupe** — control-plane result consumer stays separate (**`worker_result_key`**)
- **Not replacing Postgres state machine** — no second job database in Redis
- **Not exactly-once Kafka** — offsets + dedupe layers compose to **effective-once** side effects

---

## Related

- [redis-idempotency-deduplication.md](redis-idempotency-deduplication.md) — cross-plane dedupe overview
- [ADR-0002 Kafka choice](../decisions/ADR-0002-kafka-choice.md) — at-least-once handoff
- [worker/README.md](../../worker/README.md) — Go worker plane
- [architecture.md](../architecture.md) — control plane vs worker split
