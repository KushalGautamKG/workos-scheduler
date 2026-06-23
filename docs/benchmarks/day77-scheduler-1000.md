# KernelQ Scheduler Benchmark — Day 77

## 1. Summary

This report records a **scaled local scheduler benchmark**: **1000 jobs per trial**, **3 repeated trials**, against **Docker Compose Postgres** on a developer machine. Each trial inserts `queued` rows, then dispatches them through **`SchedulerTickRunner`** ticks (`queued` → `dispatched` only).

**No Kafka publish** (`job_producer=None`) and **no worker execution**—this measures **scheduler/Postgres claim throughput**, not end-to-end completion.

**Interview sound bite:** *“Day 77 scaled the scheduler harness to 1k jobs × 3 trials with min/avg/max throughput—local baseline data, not a production capacity claim.”*

---

## 2. Command

```bash
PYTHONPATH=. python3 control_plane/scripts/benchmark_scheduler_throughput.py \
  --count 1000 \
  --prefix day77-scheduler \
  --tenants 10 \
  --batch-size 100 \
  --trials 3
```

---

## 3. Observed Results

### Per trial

**Trial 1** (`day77-scheduler-trial-1-1782190219`):

```
generated_jobs=1000
dispatched_jobs=1000
elapsed_seconds=0.042933291999999956
jobs_dispatched_per_second=23291.94788976352
tick_count=10
```

**Trial 2** (`day77-scheduler-trial-2-1782190219`):

```
generated_jobs=1000
dispatched_jobs=1000
elapsed_seconds=0.04092349999999989
jobs_dispatched_per_second=24435.83759942338
tick_count=10
```

**Trial 3** (`day77-scheduler-trial-3-1782190219`):

```
generated_jobs=1000
dispatched_jobs=1000
elapsed_seconds=0.041071083000000064
jobs_dispatched_per_second=24348.03094917167
tick_count=10
```

### Aggregate summary

```
trials=3
generated_jobs_per_trial=1000
total_dispatched_jobs=3000
min_jobs_dispatched_per_second=23291.94788976352
avg_jobs_dispatched_per_second=24025.27214611952
max_jobs_dispatched_per_second=24435.83759942338
min_elapsed_seconds=0.04092349999999989
avg_elapsed_seconds=0.04164262499999997
max_elapsed_seconds=0.042933291999999956
event=benchmark_scheduler_throughput avg_jobs_dispatched_per_second=24025.27214611952 generated_jobs_per_trial=1000 max_jobs_dispatched_per_second=24435.83759942338 min_jobs_dispatched_per_second=23291.94788976352 total_dispatched_jobs=3000 trials=3
```

---

## 4. Interpretation

| Aspect | Detail |
|--------|--------|
| **What was measured** | **`SchedulerTickRunner`** atomic claim rate: `queued` → `dispatched` in Postgres |
| **Trials** | **3 runs** with unique prefixes; **min/avg/max** `jobs_dispatched_per_second` reported (~23.3k–24.4k jobs/sec) |
| **Scale vs Day 75** | **Larger than Day 75** (50 jobs, single trial); 1000 jobs × 10 ticks per trial exercises sustained claim loops |
| **Claim type** | **Local/dev baseline** on Docker Postgres—not production throughput or cloud capacity |

Throughput variance across trials was **~5%** (min to max), tighter than small-N smoke runs where timer noise dominates.

---

## 5. Limitations

- **No worker execution** — jobs never reach Kafka consumers or `succeeded` state in this benchmark.
- **No multi-worker load** — single synchronous scheduler loop; no Go worker pool under test.
- **No EKS / cloud** — Docker on a local machine; CPU, disk, and network differ from production.
- **Local machine noise** — background processes, Docker overhead, and shared Postgres with unrelated rows.
- **No cold vs warm comparison** — runs not separated by DB restart or cache cold-start.
- **No statistical confidence interval** — min/avg/max only; no formal variance model or many-trial distribution.

---

## 6. Next Step

Planned benchmarks after scheduler-only throughput:

1. **Worker execution throughput** — measure dispatch-to-consume and execute rates with Go workers.
2. **Multi-worker benchmark** — vary consumer count; observe dispatch vs completion balance.
3. **Bounded worker pool / backpressure** — admission limits and pool sizing under load.
4. **End-to-end completion throughput** — enqueue → dispatch → Kafka → worker → result → `succeeded` (`jobs_completed_per_second`).

See also [day75-baseline.md](day75-baseline.md), [perf.md](../perf.md), and [runbooks.md](../runbooks.md).
