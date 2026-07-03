# KernelQ Worker Throughput Benchmark — Day 91

## 1. Summary

This report records KernelQ’s **first local Go worker throughput benchmark**: dispatch messages on **`kernelq.jobs.dispatch`** → Go **`cmd/consumer`** (worker pool + bounded queue) → **`WorkerResultEvent`** on **`kernelq.jobs.results`**.

It measures **worker-side processing throughput**—not scheduler Postgres claim rate (Day 75/77) and not full enqueue-to-Postgres-`succeeded` end-to-end completion.

**Local development benchmark only. Not production capacity.**

**Day 91 baseline was intentionally conservative** — small default batch (**`COUNT=25`**), reliable completion detection, and honest local numbers before scaling trials. **Day 92** improves the harness: **per-run Kafka consumer group** (`auto.offset.reset=latest`), **prefix-isolated result polling**, and faster exit when all matching **`job_id`** values appear—without reading stale **`kernelq.jobs.results`** history. **Day 93** adds **repeated worker trials** via **`TRIALS`** (default **`1`**); each trial uses a unique prefix (`worker-bench-<timestamp>-trial-<n>`). **Future benchmark reports** should record **min/avg/max** `jobs_processed_per_second` and elapsed time across trials—not a single run only.

**Do not compare worker `jobs_processed_per_second` directly to scheduler `jobs_dispatched_per_second` (Day 75/77).** Scheduler benchmarks measure **Postgres claim only** (often no Kafka). The worker benchmark includes **Kafka dispatch intake, execution, and result publication**—a wider, slower segment by design.

**Status:** Day 91 initial harness used **`kafka-console-consumer --from-beginning`** and **under-counted** (52/100 at 120s). Intermediate fix used worker stdout; **Day 92** uses prefix-isolated **results-topic** polling. **Day 93** adds **`TRIALS`** for min/avg/max summaries. Observed numbers in §7 remain the **historical Day 91 capture** (pre–Day 92 harness); no Day 92/Day 93 re-benchmark numbers are pasted here yet.

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
| **Completion signal (Day 93+)** | Same as Day 92; **`TRIALS`** repeats the flow with per-trial prefix isolation |
| **Completion signal (Day 92)** | Per-run results consumer group; exact **`job_id`** match on **`kernelq.jobs.results`** after seek-to-latest |
| **Completion signal (Day 91 archive)** | Worker stdout or flawed `--from-beginning` poll — see §3, §7 |

---

## 5. Command

```bash
COUNT=25 WORKERS=4 QUEUE_CAPACITY=100 ./worker/scripts/benchmark_worker_throughput.sh
TRIALS=3 COUNT=25 WORKERS=4 QUEUE_CAPACITY=100 ./worker/scripts/benchmark_worker_throughput.sh
```

Defaults (when env vars unset): **`COUNT=25`**, **`WORKERS=4`**, **`QUEUE_CAPACITY=100`**, **`TRIALS=1`**, **`WAIT_TIMEOUT_SECONDS=120`**.

---

## 6. Configuration

| Setting | Value |
|---------|-------|
| **Worker count** | **4** |
| **Queue capacity** | **100** |
| **Generated jobs (default)** | **25** |
| **Backpressure** | Disabled (default) |

Job ids (Day 93+): `worker-bench-<run_id>-trial-<n>-00001` … (zero-padded). Day 92 single-run ids: `worker-bench-<run_id>-00001` … `worker-bench-<run_id>-00025`.

---

## 7. Observed Results (Day 91 archive — pre–Day 92 harness, COUNT=100)

*Historical capture only. Harness used Kafka **`--from-beginning`** polling; numbers conflate worker time with poll overhead and observation bugs. **Do not** treat as production capacity or as comparable to Day 77 scheduler throughput.*

```
generated_jobs=100
processed_jobs=100
elapsed_seconds=246.45 (approx)
jobs_processed_per_second=0.41 (approx)
```

Day 92+ harness defaults to **`COUNT=25`** with prefix-isolated result polling; **Day 93+** supports **`TRIALS`** for min/avg/max. Re-benchmark and record new tables separately from this §7 archive.

---

## 8. Limitations

**Worker throughput is local-development only. Not production capacity.**

- **Day 91 numbers (§7)** — historically accurate for that harness; conservative and not comparable to scheduler-only benchmarks.
- **Day 92 harness** — prefix-isolated results polling; still local Docker, logging executor, shared **`kernelq-worker`** dispatch group (dispatch backlog can affect elapsed time).
- **Day 93 trials** — **`TRIALS=1`** preserves single-run output; **`TRIALS>1`** prints min/avg/max. Still local dev only.
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

## 10. Day 93 Repeated Trials

| Change | Why |
|--------|-----|
| **`TRIALS`** env var (default **`1`**) | Repeated runs reduce noise from local Kafka backlog and timing variance |
| Per-trial prefix **`worker-bench-<timestamp>-trial-<n>`** | Isolate each trial’s **`job_id`** set on **`kernelq.jobs.results`** |
| Summary **`min` / `avg` / `max`** `jobs_processed_per_second` and elapsed time | Match scheduler benchmark (Day 76/77); future reports should archive these stats |

**`TRIALS=1`** keeps the original single-trial human output and structured log. **`TRIALS>1`** emits per-trial lines plus an aggregate summary and `event=benchmark_worker_throughput` with trial stats.

---

## 11. Next Steps

1. Re-run with **Day 93** harness (`TRIALS=3`, **`COUNT=25`**); record **min/avg/max** in a new report section (do not overwrite §7).
2. Scale **`COUNT=100`** with reliable prefix-isolated completion.
3. End-to-end completion benchmark.

See also [day75-baseline.md](day75-baseline.md), [day77-scheduler-1000.md](day77-scheduler-1000.md), [perf.md](../perf.md).
