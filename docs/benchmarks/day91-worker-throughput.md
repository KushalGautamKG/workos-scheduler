# KernelQ Worker Throughput Benchmark — Day 91

## 1. Summary

This report records KernelQ’s **first local Go worker throughput benchmark**: dispatch messages on **`kernelq.jobs.dispatch`** → Go **`cmd/consumer`** (worker pool + bounded queue) → **`WorkerResultEvent`** on **`kernelq.jobs.results`**.

It measures **worker-side processing throughput**—not scheduler Postgres claim rate (Day 75/77) and not full enqueue-to-Postgres-`succeeded` end-to-end completion.

**Local development benchmark only. Not production capacity.**

**Day 91 baseline was intentionally conservative** — small default batch (**`COUNT=25`**), reliable completion detection, and honest local numbers before scaling trials. **Day 92** improves the harness: **per-run Kafka consumer group** (`auto.offset.reset=latest`), **prefix-isolated result polling**, and faster exit when all matching **`job_id`** values appear—without reading stale **`kernelq.jobs.results`** history.

**Do not compare worker `jobs_processed_per_second` directly to scheduler `jobs_dispatched_per_second` (Day 75/77).** Scheduler benchmarks measure **Postgres claim only** (often no Kafka). The worker benchmark includes **Kafka dispatch intake, execution, and result publication**—a wider, slower segment by design.

**Status:** Day 91 initial harness used **`kafka-console-consumer --from-beginning`** and **under-counted** (52/100 at 120s). Intermediate fix used worker stdout; **Day 92** uses prefix-isolated **results-topic** polling. Observed numbers in §7 remain the **historical Day 91 capture** (pre–Day 92 harness).

---

## 2. Benchmark Purpose

| Question | What this benchmark answers |
|----------|----------------------------|
| How fast can the Go worker process dispatch messages? | **`jobs_processed_per_second`** from produce → all run jobs completed |
| Does the worker pool + queue config affect throughput? | Tune via **`WORKERS`** and **`QUEUE_CAPACITY`** env vars |
| How does worker throughput relate to scheduler throughput? | **Different segments** — see summary; do not rank ~0.4 worker jobs/sec against ~24k scheduler jobs/sec |

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
| **Completion signal (Day 92+)** | Per-run results consumer group; exact **`job_id`** match on **`kernelq.jobs.results`** after seek-to-latest |
| **Completion signal (Day 91 archive)** | Worker stdout or flawed `--from-beginning` poll — see §3, §7 |

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

## 7. Observed Results (Day 91 archive — pre–Day 92 harness, COUNT=100)

*Historical capture only. Harness used Kafka **`--from-beginning`** polling; numbers conflate worker time with poll overhead and observation bugs. **Do not** treat as production capacity or as comparable to Day 77 scheduler throughput.*

```
generated_jobs=100
processed_jobs=100
elapsed_seconds=246.45 (approx)
jobs_processed_per_second=0.41 (approx)
```

Day 92+ harness defaults to **`COUNT=25`** with prefix-isolated result polling; re-benchmark for updated throughput tables when ready.

---

## 8. Limitations

**Worker throughput is local-development only. Not production capacity.**

- **Day 91 numbers (§7)** — historically accurate for that harness; conservative and not comparable to scheduler-only benchmarks.
- **Day 92 harness** — prefix-isolated results polling; still local Docker, logging executor, shared **`kernelq-worker`** dispatch group (dispatch backlog can affect elapsed time).
- **Single trial** — no min/avg/max across runs yet.
- **No scheduler in path** — direct Kafka produce, not `SchedulerTickRunner`.
- **End-to-end benchmark** — enqueue → Postgres **`succeeded`** still future work.

---

## 9. Day 92 Harness Improvements

| Change | Why |
|--------|-----|
| Per-run **`kernelq-bench-results-<run_id>`** consumer group | Avoid reading stale result history |
| **`auto.offset.reset=latest`** + seek before produce | Only count results after benchmark start |
| Exact **`job_id`** grep per expected id | Prefix isolation without substring false positives |
| Faster poll loop (50ms) | Exit as soon as all matching results observed |

---

## 10. Next Steps

1. Re-run with **Day 92** harness at **`COUNT=25`**; record new observed table (separate from §7 archive).
2. Scale **`COUNT=100`** with reliable prefix-isolated completion.
3. Repeated trials — min/avg/max `jobs_processed_per_second`.
4. End-to-end completion benchmark.

See also [day75-baseline.md](day75-baseline.md), [day77-scheduler-1000.md](day77-scheduler-1000.md), [perf.md](../perf.md).
