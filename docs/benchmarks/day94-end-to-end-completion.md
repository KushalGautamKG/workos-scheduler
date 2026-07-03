# KernelQ End-to-End Completion Benchmark — Day 94

## Summary

**Day 94** adds KernelQ’s **first full-system completion benchmark**: **`queued`** jobs in Postgres → scheduler dispatch → Kafka → Go worker → **`kernelq.jobs.results`** → Python result consumer → Postgres **`succeeded`**.

**Local development benchmark only. Not production capacity.**

The harness uses **`control_plane/scripts/benchmark_end_to_end_completion.sh`** with default **`COUNT=10`** for reliability. It **complements** scheduler throughput (Day 75/77) and worker throughput (Day 91–93) benchmarks by measuring the **entire control-plane + worker loop**—not a single segment.

**Does not include the HTTP API enqueue path** (jobs are inserted via **`JobRepository`**). **Not production capacity.**

---

## Command

```bash
./control_plane/scripts/benchmark_end_to_end_completion.sh
COUNT=10 WORKERS=4 QUEUE_CAPACITY=100 TIMEOUT_SECONDS=180 ./control_plane/scripts/benchmark_end_to_end_completion.sh
```

Defaults: **`COUNT=10`**, **`WORKERS=4`**, **`QUEUE_CAPACITY=100`**, **`TIMEOUT_SECONDS=180`**.

Job ids: `e2e-bench-<timestamp>-00001` … `e2e-bench-<timestamp>-00010`.

---

## Observed Results

*Paste after first credible local run — do not overwrite historical scheduler/worker reports.*

```
# TODO: paste benchmark output (generated_jobs, dispatched_jobs, succeeded_jobs,
# jobs_completed_per_second, elapsed_seconds)
```

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
- **Single trial** — no min/avg/max yet (unlike Day 76/93 scheduler/worker trials).
- **Shared Kafka topics** — unique **`e2e-bench-*`** prefix isolates counting; historical topic traffic can still add poll noise.
- **Logging executor** — no real job I/O.

---

## Next Steps

1. Re-run locally; paste observed numbers above.
2. Add **`TRIALS`** for min/avg/max completion throughput.
3. Optional API enqueue variant (`POST /jobs` → **`succeeded`**).
4. Compare **`jobs_completed_per_second`** to scheduler-only and worker-only baselines (different segments—do not rank directly).

See also [day75-baseline.md](day75-baseline.md), [day77-scheduler-1000.md](day77-scheduler-1000.md), [day91-worker-throughput.md](day91-worker-throughput.md), [perf.md](../perf.md).
