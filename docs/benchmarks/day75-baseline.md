# KernelQ Benchmark Baseline — Day 75

## 1. Summary

This document records KernelQ’s **first local benchmark baseline**: repeatable script runs on a developer machine with Docker Compose infrastructure. It captures **insertion throughput**, **scheduler dispatch throughput**, and **queue-wait percentile plumbing**—enough to validate measurement harnesses and compare future runs, **not** to claim production capacity.

**Interview sound bite:** *“Day 75 established labeled local baselines for enqueue, dispatch, and latency metrics before scaling job counts, Kafka-in-the-path runs, and end-to-end completion benchmarks.”*

---

## 2. Environment

| Item | Detail |
|------|--------|
| **Machine** | Local development machine (macOS) |
| **Infrastructure** | Docker Compose (`postgres`, `zookeeper`, `kafka`; Prometheus/Grafana optional) |
| **Postgres** | Local container (`kernelq-postgres`), migrations applied, shared DB with seed/smoke/benchmark rows |
| **Kafka** | Local container available for smoke tests; **scheduler throughput benchmark used `job_producer=None`** (Postgres claim only) |
| **Control plane** | Python scripts via `PYTHONPATH=.` (`generate_load_jobs.py`, `benchmark_scheduler_throughput.py`, `seed_latency_metrics.py`, `job_duration_snapshot.py`) |
| **Workers** | Go worker **unit/integration tests** pass; **no worker execution throughput benchmark yet** |

Record commit SHA, hardware, and Docker versions when reproducing or extending these numbers.

---

## 3. Benchmarks Run

### A) Load generation benchmark

**Purpose:** Measure **Postgres job creation** throughput (`queued` rows via `JobRepository.create_job`).

**Command:**

```bash
PYTHONPATH=. python3 control_plane/scripts/generate_load_jobs.py --count 25 --prefix day73-smoke --tenants 3 --max-priority 20
```

**Observed:**

```
created_jobs=25
elapsed_seconds=0.049740625
jobs_per_second=502.60727524030915
event=generate_load_jobs ...
```

### B) Scheduler throughput benchmark

**Purpose:** Measure **`queued` → `dispatched`** throughput via repeated `SchedulerTickRunner` ticks (no Kafka publish).

**Command:**

```bash
PYTHONPATH=. python3 control_plane/scripts/benchmark_scheduler_throughput.py --count 50 --prefix day74-bench --tenants 5 --batch-size 10
```

**Observed:**

```
generated_jobs=50
dispatched_jobs=50
elapsed_seconds=0.007508457999999996
jobs_dispatched_per_second=6659.156913443483
tick_count=5
event=benchmark_scheduler_throughput ...
```

### C) Queue wait percentile seed

**Purpose:** Validate **latency metric plumbing** (`dispatched_at - created_at` → p50/p95/p99) after seeding succeeded jobs with varied queue waits.

**Setup:** `PYTHONPATH=. python3 control_plane/scripts/seed_latency_metrics.py`, then `job_duration_snapshot.py` or `GET /metrics/durations`.

**Observed (representative snapshot after seed):**

```
p50_queue_wait_seconds=3.0
p95_queue_wait_seconds=10.0
p99_queue_wait_seconds=10.0
```

Percentiles are **snapshot-derived** from Postgres (nearest-rank), not Prometheus histogram buckets.

---

## 4. Interpretation

| Measurement | What it means |
|-------------|----------------|
| **`jobs_per_second`** (load generator) | How fast the control plane can **insert** `queued` jobs into Postgres—not HTTP enqueue, not dispatch, not worker execution. |
| **`jobs_dispatched_per_second`** (scheduler benchmark) | How fast **`SchedulerTickRunner`** can **claim** jobs (`queued` → `dispatched`) in Postgres for this prefix and batch size. |
| **Queue wait p50/p95/p99** | Confirms **duration metrics** and Prometheus gauge export reflect realistic waits after synthetic seed data. |

These numbers are **local baselines** on a shared dev database: useful for regression checks and demo narratives. They are **not** production SLOs, capacity plans, or cloud performance claims.

---

## 5. Limitations

- **Small job counts** (25 insert, 50 dispatch)—timer noise and warm caches dominate; not sustained load.
- **No repeated trials**—single runs only; no min/median/max or variance reported *(Day 75 snapshot; see Day 76 follow-up below)*.
- **No multi-worker execution benchmark**—Go workers tested in CI, not throughput-measured under load.
- **No worker pool / backpressure benchmark**—admission and consumer scaling not exercised.
- **No p95 execution latency**—queue-wait percentiles seeded; worker execution and end-to-end completion latency not benchmarked.
- **Not AWS/EKS**—Docker on a laptop; network, disk, and CPU differ from production.
- **Local machine noise**—background processes, shared Postgres with unrelated rows, and Docker overhead affect repeatability.

---

## 6. Next Benchmarks

**Day 76 follow-up:** `benchmark_scheduler_throughput.py` adds **`--trials`** with **min/avg/max** `jobs_dispatched_per_second` across repeated runs (unique prefix per trial). **Future benchmark reports** (Day 76 and later) should record **trial count**, **min/avg/max throughput**, and **machine environment** in addition to raw observed numbers. This Day 75 report is unchanged—single-trial scheduler results only.

**Day 77:** Larger scheduler benchmark — [day77-scheduler-1000.md](day77-scheduler-1000.md) (1000 jobs per trial, 3 trials).

Planned extensions to support **resume-quality throughput and latency claims**:

1. **1k / 10k load generation** — insertion throughput at realistic queue depth.
2. **Scheduler throughput repeated trials** — *partially addressed Day 76 (`--trials`)*; full archived reports with environment metadata and Kafka publish path still TODO.
3. **Worker pool throughput** — vary Go consumer count; measure dispatch-to-consume and execute rates.
4. **End-to-end completion throughput** — enqueue → dispatch → Kafka → worker → result → `succeeded` (`jobs_completed_per_second`).
5. **p95 / p99 queue and execution latency** — histogram or repeated snapshot percentiles under load.
6. **Kafka replay / crash recovery tests** — broker outages, stranded `dispatched` rows, and recovery behavior.

See also [perf.md](../perf.md) and [runbooks.md](../runbooks.md) (Performance / Benchmarking).
