# Kafka Pause/Resume Backpressure Design

Design for the next worker backpressure layer after Day 82 local backoff.

**Implementation status**

| Day | Delivered |
|-----|-----------|
| **85** | **`BackpressurePolicy`** — high/low watermarks on depth vs capacity |
| **86** | **`PauseResumeController`** boundary + **`InMemoryPauseResumeController`** |
| **87** | **`KafkaConsumer.Run`** wires policy → controller (`maybeApplyBackpressure`); **`backpressure_pause_events`** / **`backpressure_resume_events`** in shutdown stats |
| **Future** | Real Kafka partition **`Pause`/`Resume`** adapter (replaces in-memory controller in production) |

The **adapter** keeps Kafka-specific calls out of policy logic; the **in-memory controller** supports deterministic tests without a broker.

---

## 1. Problem

The Go worker pool uses a **bounded in-process queue** between the Kafka poll loop and executor goroutines (`KERNELQ_WORKER_QUEUE_CAPACITY`, default **100**).

When **Kafka intake outpaces execution**, the queue saturates:

- Poll loop decodes faster than workers finish jobs.
- Memory in the worker process stays bounded, but pressure shows up as **`work_queue_full_errors`** and dropped or retried enqueues.

**Day 82 local backoff** (50ms sleep + one retry) eases burst enqueue pressure inside the process. It does **not** stop Kafka from delivering messages into the consumer client. Fetch buffers can still fill; saturation remains possible after retry fails.

**Goal:** propagate backpressure to the **broker boundary** — stop pulling new records when the local queue is high, resume when it has headroom.

---

## 2. Current Behavior

Today (`worker/internal/worker/kafka_consumer.go`, Day 79–82):

| Piece | Behavior |
|-------|----------|
| **Bounded queue** | Buffered channel between poll loop and worker pool (default capacity **100**). |
| **`work_queue_full_errors`** | Incremented when `Enqueue` returns `worker queue full`. |
| **`event=worker_queue_full`** | Structured log: `job_id`, `queue_capacity`. |
| **Backoff** | On queue full: log + increment counter → **50ms sleep** → **one retry** → drop if still full (no DLQ). |

Poll loop **keeps running**; workers **keep executing** in-flight and queued jobs. Shutdown stats include `work_queue_capacity`, `work_queue_depth`, `work_items_enqueued`, `work_queue_full_errors`, `backpressure_pause_events`, `backpressure_resume_events`.

**Day 85 — `BackpressurePolicy`:** default **high 80%** / **low 50%**; **`ShouldPause` / `ShouldResume`** with hysteresis.

**Day 86 — `PauseResumeController`:** **`InMemoryPauseResumeController`** for tests; idempotent **`Pause`/`Resume`**.

**Day 87 — wiring:** when **`BackpressurePolicy`** and **`PauseResumeController`** are both set on **`KafkaConsumer`**, **`maybeApplyBackpressure`** evaluates **`QueueDepth()`** / **`QueueCapacity()`** before poll, after enqueue, and after worker success—calls controller and logs **`event=worker_backpressure_pause`** / **`event=worker_backpressure_resume`**. **`cmd/consumer`** leaves both nil by default. Controller is still **in-memory**; real Kafka partition pause/resume is **future work**.

**Smoke:** `./worker/scripts/smoke_queue_saturation.sh` runs `TestQueueFull*` (no real Kafka).

---

## 3. Proposed Behavior

Add a **queue-depth policy** that drives Kafka **pause/resume** on assigned partitions:

1. **High watermark** — when depth ≥ high threshold, call `Pause(partitions)` (stop fetching new records).
2. **Low watermark** — when depth ≤ low threshold, call `Resume(partitions)`.
3. **Workers keep running** during pause — only intake stops; in-flight and buffered jobs still execute.
4. **Health / shutdown unchanged** — SIGINT/SIGTERM still drain or shut down the pool; pause state must not block clean exit.

Local backoff may remain as a **last-resort** safety net for races between depth check and enqueue; primary control moves to pause/resume.

```
Kafka broker
    ↓  (pause stops fetch here)
Poll loop → decode → Enqueue → [bounded queue] → worker pool → executor
              ↑
        depth policy (high/low watermarks)
```

---

## 4. Watermarks

**`BackpressurePolicy`** encodes defaults: **high 80%**, **low 50%** (invalid config → same defaults).

Depth = **jobs waiting in the bounded queue** (`WorkerPool.QueueDepth()`).

**Example** (capacity **100**):

| Threshold | Level | Action |
|-----------|-------|--------|
| **High watermark** | **80** (80%) | Pause all assigned partitions |
| **Low watermark** | **50** (50%) | Resume all assigned partitions |

**Hysteresis:** two thresholds prevent **flapping**. With a single threshold at 80, one dequeue to 79 would resume, the next poll would hit 80 and pause again — oscillating pause/resume. The gap between 80 (pause) and 50 (resume) is a dead band: intake stays off until meaningful headroom returns.

Configurable via env (names TBD), e.g. `KERNELQ_QUEUE_HIGH_WATERMARK_PCT` / `KERNELQ_QUEUE_LOW_WATERMARK_PCT`.

---

## 5. Metrics

Future Prometheus / shutdown counters:

| Metric | Purpose |
|--------|---------|
| `kafka_partitions_paused` | Current count of partitions in paused state |
| `kafka_pause_events` | Total pause transitions (saturation episodes) |
| `kafka_resume_events` | Total resume transitions (recovery episodes) |
| `worker_queue_depth` | Current bounded-queue occupancy |
| `worker_queue_capacity` | Configured capacity (already in shutdown stats) |
| `work_queue_full_errors` | Residual saturation after pause policy (should trend toward **0**) |

**Proof pause/resume helps:** under sustained overload, `work_queue_full_errors` drops while consumer **lag** absorbs the burst (expected) and process memory stays stable.

---

## 6. Failure Modes

| Mode | Risk | Mitigation |
|------|------|------------|
| **Never resuming** | Stuck paused, lag grows unbounded, throughput zero | Low-watermark check on every dequeue completion; watchdog / alert on `kafka_partitions_paused > 0` for too long |
| **Pausing too aggressively** | Low watermark too high or high watermark too low → chronic under-utilization | Tune thresholds; measure worker idle % vs lag |
| **Queue never drains** | Slow/stuck executor; pause does not fix root cause | Existing handler timeouts, ops alerts on depth; pause is not a substitute for fixing execution |
| **Rebalance while paused** | Partition assignment changes; stale pause state on new partitions | On `RevokedPartitions` / `AssignedPartitions`, reset pause map; resume new assignments by default, re-evaluate depth |
| **Shutdown while paused** | Missed commits or hung shutdown | Shutdown path forces resume or commits offsets for in-flight work; pool `Shutdown()` drains queue regardless of pause flag |

---

## 7. Implementation Plan

1. ~~**Expose queue depth**~~ — `WorkerPool.QueueDepth()` (Day 84).
2. ~~**Pause/resume interface**~~ — **`PauseResumeController`** + in-memory fake (Day 86).
3. ~~**Policy module**~~ — **`BackpressurePolicy`** (Day 85).
4. ~~**Wire policy → controller in `Run`**~~ — `maybeApplyBackpressure` (Day 87); **Kafka adapter** still future work.
5. ~~**Policy integration tests**~~ — deterministic depth transitions (Day 87).
6. **Smoke test** — `./worker/scripts/smoke_kafka_pause_resume.sh` (or extend saturation smoke).
7. **Prometheus metrics** — counters/gauges from §5.
8. **Grafana dashboard** — depth, pause/resume events, `work_queue_full_errors`, consumer lag panel.
9. **Kafka adapter** — real partition **`Pause`/`Resume`** replacing in-memory controller in **`cmd/consumer`**.

---

## 8. Non-Goals

- **Real Kafka partition pause/resume not wired yet** — Day 87 uses in-memory controller only; broker adapter is next.
- **Not replacing Redis idempotency** — pause/resume is flow control, not deduplication.
- **Not changing retry / DLQ semantics** — invalid messages, terminal failures, and dead-letter routing stay as-is.
- **Not autoscaling** — separate future work (pool size / replica count).

---

**Interview sound bite:** *“Bounded queue bounds memory; local backoff reacts after failure; Kafka pause/resume stops fetch at the broker when depth crosses a high watermark and resumes below a lower watermark so we don’t flap. Success means queue-full errors vanish while lag absorbs overload instead of drops.”*
