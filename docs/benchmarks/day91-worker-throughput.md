# KernelQ Worker Throughput Benchmark — Day 91

## 1. Summary

This report records KernelQ’s **first local Go worker throughput benchmark**: dispatch messages on **`kernelq.jobs.dispatch`** → Go **`cmd/consumer`** (worker pool + bounded queue) → **`WorkerResultEvent`** on **`kernelq.jobs.results`**.

It measures **worker-side processing throughput**—not scheduler Postgres claim rate (Day 75/77) and not full enqueue-to-Postgres-`succeeded` end-to-end completion.

**Local development benchmark only. Not production capacity.**

**Interview sound bite:** *“Day 91 measured dispatch-to-result throughput through the Go worker pool on local Kafka—about 0.4 jobs/sec in this harness, dominated by polling overhead—not a production SLO.”*

---

## 2. Benchmark Purpose

| Question | What this benchmark answers |
|----------|----------------------------|
| How fast can the Go worker process dispatch messages? | **`jobs_processed_per_second`** from produce → all results visible |
| Does the worker pool + queue config affect throughput? | Tune via **`WORKERS`** and **`QUEUE_CAPACITY`** env vars |
| How does worker throughput compare to scheduler throughput? | Scheduler (Day 77) claims in Postgres at **~24k jobs/sec** locally; worker path is **Kafka + execution + result publish**—orders of magnitude different segment |

**What it does not measure:** API enqueue, Postgres scheduling, Python result consumer, retry/DLQ paths, or terminal state updates in Postgres.

---

## 3. Environment

| Item | Detail |
|------|--------|
| **Machine** | Local development machine (macOS) |
| **Infrastructure** | Docker Compose (`zookeeper`, `kafka`) |
| **Kafka** | `kernelq-kafka` container; topics via `./infra/kafka/create-topics.sh` |
| **Worker** | `worker/cmd/consumer` binary; logging executor (no real job I/O) |
| **Harness** | `./worker/scripts/benchmark_worker_throughput.sh` |
| **Consumer group** | `kernelq-worker` (fixed in `main.go`) |

Record commit SHA, hardware, and Docker versions when reproducing.

---

## 4. Command

```bash
COUNT=100 WORKERS=4 QUEUE_CAPACITY=100 ./worker/scripts/benchmark_worker_throughput.sh
```

Defaults (when env vars unset): **`COUNT=100`**, **`WORKERS=4`**, **`QUEUE_CAPACITY=100`**.

---

## 5. Configuration

| Setting | Value |
|---------|-------|
| **Worker count** (`KERNELQ_WORKER_COUNT`) | **4** |
| **Queue capacity** (`KERNELQ_WORKER_QUEUE_CAPACITY`) | **100** |
| **Generated jobs** | **100** |
| **Backpressure** | Disabled (default) |

Job ids use deterministic zero-padded suffixes under a unique run prefix: `worker-bench-<run_id>-00001` … `worker-bench-<run_id>-00100`.

---

## 6. Observed Results

```
generated_jobs=100
processed_jobs=100
elapsed_seconds=246.44901776313782
jobs_processed_per_second=0.4057634350002158
worker_count=4
queue_capacity=100
job_prefix=worker-bench-1782880479
event=benchmark_worker_throughput generated_jobs=100 processed_jobs=100 elapsed_seconds=246.44901776313782 jobs_processed_per_second=0.4057634350002158 worker_count=4 queue_capacity=100 job_prefix=worker-bench-1782880479
```

| Metric | Value |
|--------|-------|
| **Generated jobs** | 100 |
| **Processed jobs** | 100 |
| **Elapsed time** | ~246.4 s |
| **Jobs/sec** | ~0.41 |

---

## 7. Interpretation

| Measurement | What it means |
|-------------|----------------|
| **`generated_jobs`** | Dispatch events produced to **`kernelq.jobs.dispatch`** |
| **`processed_jobs`** | Matching **`WorkerResultEvent`** records found on **`kernelq.jobs.results`** for this run’s job prefix |
| **`elapsed_seconds`** | Wall time from batch produce start until all results are observed |
| **`jobs_processed_per_second`** | `processed_jobs / elapsed_seconds` |

The observed **~0.4 jobs/sec** is **not** intrinsic worker execution speed. The harness polls results by repeatedly running **`kafka-console-consumer --from-beginning`** inside Docker—each poll scans topic history and adds **seconds of overhead per iteration**. The logging executor itself is near-instant; the benchmark measures **worker path + observation cost**.

Compare to **Day 77 scheduler** (~24k `jobs_dispatched_per_second` Postgres-only): different layer, different bottleneck, different harness.

---

## 8. Limitations

**Local development benchmark only. Not production capacity.**

- **Single trial** — no min/avg/max across repeated runs yet.
- **Result polling overhead** — `kafka-console-consumer` full-topic scans dominate elapsed time; not suitable for sub-second throughput claims.
- **Logging executor only** — no real job I/O, network calls, or Postgres updates from the worker.
- **No Python result consumer** — does not measure control-plane feedback or `succeeded` in Postgres.
- **No scheduler in path** — jobs are produced directly to Kafka, not via `SchedulerTickRunner`.
- **Shared consumer group** — `kernelq-worker` may replay or skip offsets from prior local runs; unique job prefixes isolate counting but not broker load.
- **Stale topic data** — `kernelq.jobs.results` retains history; polls read from beginning.
- **Not AWS/EKS** — Docker on a laptop; network, disk, and CPU differ from production.
- **COUNT=100 required `WAIT_TIMEOUT_SECONDS=300`** on this machine for one successful capture; default 120s timed out at 52/100 in an earlier attempt—environment sensitivity.

---

## 9. Next Benchmarks

1. **Repeated trials** — min/avg/max `jobs_processed_per_second` (mirror Day 76/77 scheduler harness).
2. **Lower observation overhead** — consumer-group lag, worker shutdown `messages_processed`, or dedicated results counter.
3. **Vary `WORKERS` / `QUEUE_CAPACITY`** — pool sizing under load.
4. **Backpressure enabled** — `KERNELQ_WORKER_BACKPRESSURE_*` env impact on throughput.
5. **End-to-end completion benchmark** — enqueue → scheduler → worker → result consumer → Postgres `succeeded`.

See also [day75-baseline.md](day75-baseline.md), [day77-scheduler-1000.md](day77-scheduler-1000.md), [perf.md](../perf.md), and [checkpoints/day90.md](../checkpoints/day90.md).
