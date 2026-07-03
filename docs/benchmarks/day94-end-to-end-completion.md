# KernelQ End-to-End Completion Benchmark — Day 94

## Summary

**Day 94** adds KernelQ’s **first full-system completion benchmark**: **`queued`** jobs in Postgres → scheduler dispatch → Kafka → Go worker → **`kernelq.jobs.results`** → Python result consumer → Postgres **`succeeded`**.

**Local development benchmark only. Not production capacity.**

The harness uses **`control_plane/scripts/benchmark_end_to_end_completion.sh`** with default **`COUNT=10`** for reliability. It **complements** scheduler throughput (Day 75/77) and worker throughput (Day 91–93) benchmarks by measuring the **entire control-plane + worker loop**—not a single segment.

**Day 95** adds **repeated E2E trials** via **`TRIALS`** (default **`1`**); each trial uses a unique prefix (`e2e-bench-<timestamp>-trial-<n>`). **Future benchmark reports** should record **min/avg/max** `jobs_completed_per_second` and elapsed time across trials.

**Does not include the HTTP API enqueue path** (jobs are inserted via **`JobRepository`**). **Not production capacity.**

---

## Command

```bash
./control_plane/scripts/benchmark_end_to_end_completion.sh
COUNT=10 WORKERS=4 QUEUE_CAPACITY=100 TIMEOUT_SECONDS=180 ./control_plane/scripts/benchmark_end_to_end_completion.sh
TRIALS=2 COUNT=5 WORKERS=4 QUEUE_CAPACITY=100 ./control_plane/scripts/benchmark_end_to_end_completion.sh
```

Defaults: **`COUNT=10`**, **`WORKERS=4`**, **`QUEUE_CAPACITY=100`**, **`TRIALS=1`**, **`TIMEOUT_SECONDS=180`**.

Job ids (Day 95+): `e2e-bench-<timestamp>-trial-<n>-00001` … (zero-padded). Day 94 single-run ids: `e2e-bench-<timestamp>-00001` … `e2e-bench-<timestamp>-00010`.

---

## Observed Results

### Day 94 archive (single trial, COUNT=10)

*Historical capture — pre–Day 95 harness prefix (`e2e-bench-<timestamp>` without `-trial-<n>`).*

```
generated_jobs=10
dispatched_jobs=10
succeeded_jobs=10
elapsed_seconds=20.27 (approx)
jobs_completed_per_second=0.49 (approx)
```

Day 95+ re-benchmarks should record **min/avg/max** in a new section; do not overwrite this archive.

---

## What This Measures

| Segment | Included |
|---------|----------|
| Postgres **`queued`** job rows | Yes (repository insert) |
| Scheduler claim + Kafka dispatch publish | Yes |
| Go worker consume + execute + result publish | Yes |
| Python result consumer + Postgres **`succeeded`** update | Yes |
| HTTP API enqueue | **No** |
| Retry / DLQ paths | **No** |

**Metric:** **`jobs_completed_per_second`** = `succeeded_jobs / elapsed_seconds` from first scheduler tick through all jobs **`succeeded`**.

---

## Limitations

- **Local development only** — Docker Compose Postgres + Kafka on a single machine.
- **Repository insert path** — not **`POST /jobs`**; API latency excluded.
- **Day 94 numbers (above)** — historically accurate for the first single-trial harness.
- **Day 95 trials** — **`TRIALS=1`** preserves single-trial output; **`TRIALS>1`** prints min/avg/max.
- **Shared Kafka topics** — unique **`e2e-bench-*`** prefix isolates counting; historical topic traffic can still add poll noise.
- **Logging executor** — no real job I/O.

---

## Next Steps

1. Re-run with **Day 95** harness (`TRIALS=3`, **`COUNT=10`**); record min/avg/max separately from Day 94 archive.
2. Optional API enqueue variant (`POST /jobs` → **`succeeded`**).
3. Compare **`jobs_completed_per_second`** to scheduler-only and worker-only baselines (different segments—do not rank directly).

See also [day75-baseline.md](day75-baseline.md), [day77-scheduler-1000.md](day77-scheduler-1000.md), [day91-worker-throughput.md](day91-worker-throughput.md), [perf.md](../perf.md).
