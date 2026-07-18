# Execution Recovery Design

Design for the **claim-before-completion gap**: Redis execution claim succeeds, then the worker dies before execute and/or result publish. Duplicate suppression works; the job can still stall.

**Interview sound bite:** *“Idempotency keys stop double work; leases + a watchdog reclaim abandoned work. Fail closed on duplicates; recover open on orphans.”*

---

## 1. Problem

Worker execution idempotency claims **`execution:<job_id>:<attempt>`** with Redis **`SET NX EX`** *before* **`Executor.Execute`**. That correctly prevents double side effects under Kafka replay.

It also creates a failure window:

```
Redis TryClaim
      │
      ▼
Worker crashes
      │
      ▼
No execution · No result · Redis key still exists
```

After restart, a replay of the same dispatch sees **`TryClaim → false`** and **skips** execution. The attempt never finishes until the key expires or an operator intervenes.

This is **not** a Redis bug. It is the cost of **failing closed** on duplicates when the claim is only a boolean “seen” flag.

---

## 2. Timeline

```
Worker
  │
  ▼
TryClaim()                    ← first claimant wins
  │
  ▼
Redis stores execution key    ← TTL clock starts; no “completed” bit
  │
  ▼
Worker crashes                ← before Execute and/or PublishResult
  │
  ▼
No result published
  │
  ▼
Replay arrives
  │
  ▼
TryClaim() → false            ← treated as healthy duplicate
  │
  ▼
Worker skips Execute
  │
  ▼
Job stalled                   ← Postgres may still show dispatched / no terminal state
```

Contrast with the happy path: claim → execute → publish result → control plane updates Postgres.

---

## 3. Current Behavior

| Rule | Status |
|------|--------|
| **Fail closed on duplicate claim** | **Yes** — second claim skips **`Execute`** (`duplicate_skipped`) |
| **Duplicate execution prevented** | **Yes** — Day 112–114 |
| **Automatic recovery of abandoned claims** | **No** — intentionally deferred |
| **Crash-after-claim demo** | **Day 115** — **`smoke_execution_claim_gap.sh`** (educational; does not fix) |

KernelQ **intentionally fails closed**: prefer “job may wait” over “job may run twice.” Production systems must add an explicit recovery boundary; Day 115 documents that boundary without implementing it.

---

## 4. Possible Recovery Strategies

Discuss only — **do not implement** in Day 115.

### Execution leases

Store claim metadata (owner id, `claimed_at`, lease deadline) instead of a bare `"1"`. A live worker renews the lease while running; expiry means the attempt is reclaimable.

### Heartbeats

Periodic Redis refresh (or separate heartbeat key) while **`Execute`** is in progress. Missed heartbeats mark the claim abandoned without waiting for the full dedupe TTL.

### Claim expiration

Shorter claim TTL than the full replay window, or a dedicated lease TTL. Trades earlier reclaim against a higher chance of overlapping double execution if work outlives the lease.

### Execution ownership

Value includes **`worker_id` / generation**. Only the owner may complete or release; a watchdog may steal after lease expiry. Prevents two healthy workers from both thinking they own the attempt.

### Watchdog recovery

Background process scans “claimed but not completed” attempts (Redis lease expired **and** Postgres still non-terminal / no matching result) and either deletes the claim or marks it reclaimable.

### Reconciliation scanner

Compare Redis execution keys to Postgres/`kernelq.jobs.results`. Stale claims with no result → delete or rewrite. Complements leases; Postgres remains source of truth for job state.

---

## 5. Recommendation

**Execution lease + watchdog scanner.**

1. **Claim as a lease** — `SET NX` with owner + expiry; renew while executing.
2. **Complete or release** — on successful result publish (or explicit abandon), delete/overwrite so replays are unambiguous.
3. **Watchdog** — periodically find expired leases whose jobs are still incomplete in Postgres; reclaim so a later dispatch can run once.

Why this pair:

- Preserves **fail-closed duplicate suppression** for healthy replays while the lease is live.
- Adds **liveness** for crash-after-claim without treating every skip as success.
- Keeps Redis a **cache/lease plane**; Postgres stays authoritative.

Defer OpenTelemetry spans, gRPC, and CloudWatch alerts for lease/watchdog metrics to later production-readiness work.

---

## 6. Non-Goals (Day 115)

- **Not implementing** leases, heartbeats, watchdog, or reclaim APIs
- **Not changing** default **`TryClaim`** boolean semantics
- **Not relaxing** fail-closed duplicate skip
- **Not replacing** dispatch or result idempotency layers

---

## Related

- [worker-execution-idempotency.md](worker-execution-idempotency.md) — execution claim before **`Execute`**
- [redis-idempotency-deduplication.md](redis-idempotency-deduplication.md) — cross-plane dedupe overview
- [day115.md](../checkpoints/day115.md) — checkpoint: dedupe complete; recovery deferred
- `worker/scripts/smoke_execution_claim_gap.sh` — demonstrates the gap (no fix)
