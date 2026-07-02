# KernelQ Worker Throughput Benchmark — Day 91

## 1. Summary

This report records KernelQ’s **first local Go worker throughput benchmark**: dispatch messages on **`kernelq.jobs.dispatch`** → Go **`cmd/consumer`** (worker pool + bounded queue) → **`WorkerResultEvent`** on **`kernelq.jobs.results`**.

It measures **worker-side processing throughput**—not scheduler Postgres claim rate (Day 75/77) and not full enqueue-to-Postgres-`succeeded` end-to-end completion.

**Local development benchmark only. Not production capacity.**

**Status:** Initial harness used **`kafka-console-consumer --from-beginning`** to count results; that **under-counted** completions on a topic with history (52/100 at 120s timeout). Harness now waits on **prefix-isolated `received task` lines** in worker stdout. Re-benchmark after fix before treating numbers as authoritative.

---

## 2. Benchmark Purpose

| Question | What this benchmark answers |
|----------|----------------------------|
| How fast can the Go worker process dispatch messages? | **`jobs_processed_per_second`** from produce → all run jobs completed |
| Does the worker pool + queue config affect throughput? | Tune via **`WORKERS`** and **`QUEUE_CAPACITY`** env vars |
| How does worker throughput compare to scheduler throughput? | Scheduler (Day 77) claims in Postgres at **~24k jobs/sec** locally; worker path is **Kafka + execution + result publish**—different segment |

**What it does not measure:** API enqueue, Postgres scheduling, Python result consumer, retry/DLQ paths, or terminal state updates in Postgres.

---

## 3. Root Cause (52/100 false timeout)

| Suspected cause | Verdict |
|-----------------|---------|
| Worker still processing | Partially — at 120s some runs were still in flight, but worker often finished faster |
| **Result polling reads stale head of topic** | **Yes** — `kafka-console-consumer --from-beginning --max-messages N` returns **oldest** results first; new benchmark results at the **log tail** were missed as **`kernelq.jobs.results`** grew |
| Incomplete prefix filtering | Possible secondary issue; fixed by **exact `job_id` grep** per expected id |
| Queue drops | Not observed in logs for test runs (`work_queue_full_errors=0`) |
| Prefix isolation | Run prefix was unique; issue was **observation**, not worker mix-up |

**Inspect stale reads:**

```bash
docker exec kernelq-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic kernelq.jobs.results \
  --from-beginning \
  --max-messages 20
```

Shows **historical** `worker-bench-*` rows—not necessarily the latest run.

---

## 4. Environment

| Item | Detail |
|------|--------|
| **Machine** | Local development machine (macOS) |
| **Infrastructure** | Docker Compose (`zookeeper`, `kafka`) |
| **Worker** | `worker/cmd/consumer` binary; logging executor |
| **Harness** | `./worker/scripts/benchmark_worker_throughput.sh` |
| **Completion signal** | Worker stdout: `received task job_id=<prefix>-NNNNN` |

---

## 5. Command

```bash
COUNT=25 WORKERS=4 QUEUE_CAPACITY=100 ./worker/scripts/benchmark_worker_throughput.sh
```

Defaults (when env vars unset): **`COUNT=25`**, **`WORKERS=4`**, **`QUEUE_CAPACITY=100`**, **`WAIT_TIMEOUT_SECONDS=120`**.

---

## 6. Configuration

| Setting | Value |
|---------|-------|
| **Worker count** | **4** |
| **Queue capacity** | **100** |
| **Generated jobs (default)** | **25** |
| **Backpressure** | Disabled (default) |

Job ids: `worker-bench-<run_id>-00001` … `worker-bench-<run_id>-00025` (zero-padded).

---

## 7. Observed Results (pre-fix harness, COUNT=100)

*Archive only — harness used Kafka topic polling; numbers conflate worker time with poll overhead.*

```
generated_jobs=100
processed_jobs=100
elapsed_seconds=246.45 (approx)
jobs_processed_per_second=0.41 (approx)
```

Re-run with fixed harness and default **`COUNT=25`** before updating this section.

---

## 8. Limitations

**Local development benchmark only. Not production capacity.**

- **Harness reliability** — fixed after Day 91 initial commit; do not use `--from-beginning` result counts on busy topics.
- **Single trial** — no min/avg/max across runs yet.
- **Logging executor only** — no real job I/O or Postgres updates.
- **No scheduler in path** — direct Kafka produce, not `SchedulerTickRunner`.
- **Shared consumer group** — `kernelq-worker`; unique job prefixes isolate counting.
- **End-to-end benchmark** — still future work.

---

## 9. Next Steps

1. Re-run fixed harness at **`COUNT=25`**; confirm **`processed_jobs == generated_jobs`** under default timeout.
2. Scale **`COUNT=100`** with reliable completion detection.
3. Repeated trials — min/avg/max `jobs_processed_per_second`.
4. End-to-end completion benchmark.

See also [day75-baseline.md](day75-baseline.md), [day77-scheduler-1000.md](day77-scheduler-1000.md), [perf.md](../perf.md).
