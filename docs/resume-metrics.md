# Resume Metrics Evidence Inventory (Day 130)

**Rule:** Resume numbers must come from committed benchmark evidence. Aspirational or unsupported figures are labeled and must not be used as if measured.

Labels: **Verified** · **Derivable from committed evidence** · **Requires new experiment** · **Unsupported**

## Benchmark inventory

| Area | Report |
|------|--------|
| Scheduler throughput (small) | [day75-baseline.md](benchmarks/day75-baseline.md) |
| Scheduler throughput (1k × 3) | [day77-scheduler-1000.md](benchmarks/day77-scheduler-1000.md) |
| Worker throughput | [day91-worker-throughput.md](benchmarks/day91-worker-throughput.md) |
| End-to-end completion | [day94-end-to-end-completion.md](benchmarks/day94-end-to-end-completion.md) |
| Latency plumbing | day75 + queue-wait metrics docs |
| Replay / idempotency | [day114-kafka-execution-replay.md](benchmarks/day114-kafka-execution-replay.md) |
| Resilience recovery | [day129-resilience.md](benchmarks/day129-resilience.md) |

## Candidate resume numbers

### Scheduler claim throughput (local)

| Field | Value |
|-------|--------|
| Metric | Jobs dispatched/sec (Postgres claim only) |
| Observed value | min ≈ 23.3k, avg ≈ 24.0k, max ≈ 24.4k |
| Baseline | Day 75: ~6.7k jobs/sec at count=50 (different scale) |
| Comparison value | Day 77 1k×3 trials |
| Benchmark command | `PYTHONPATH=. python3 control_plane/scripts/benchmark_scheduler_throughput.py --count 1000 --trials 3 ...` |
| Report path | `docs/benchmarks/day77-scheduler-1000.md` |
| Environment | Local Docker Postgres; **no Kafka** |
| Limitations | Not end-to-end; not production capacity |
| Safe to use on resume? | **Yes**, with “local scheduler claim throughput” wording |
| Label | **Verified** |

### Worker processing throughput (local)

| Field | Value |
|-------|--------|
| Metric | Jobs processed/sec (Kafka → worker → result) |
| Observed value | Day 91 archive ≈ 0.41 jobs/sec (COUNT=100; flawed observation harness noted) |
| Baseline | N/A comparable to scheduler-only |
| Benchmark command | `./worker/scripts/benchmark_worker_throughput.sh` |
| Report path | `docs/benchmarks/day91-worker-throughput.md` |
| Environment | Local Docker Kafka; logging executor |
| Limitations | Historical harness caveats; Day 92/93 re-bench numbers not pasted |
| Safe to use on resume? | **Only with caveats**; prefer qualitative “worker throughput harness exists” unless re-run |
| Label | **Verified** (archive) / **Requires new experiment** for clean Day 93 min/avg/max quote |

### End-to-end completion rate (local)

| Field | Value |
|-------|--------|
| Metric | Jobs completed/sec (queued → succeeded) |
| Observed value | Day 94: 10/10 succeeded ≈ 0.49 jobs/sec |
| Benchmark command | `./control_plane/scripts/benchmark_end_to_end_completion.sh` |
| Report path | `docs/benchmarks/day94-end-to-end-completion.md` |
| Environment | Local full stack |
| Limitations | Small N; not API enqueue path |
| Safe to use on resume? | **Yes** as “local E2E completion benchmark (N=10)” |
| Label | **Verified** |

### Duplicate Kafka replay (functional)

| Field | Value |
|-------|--------|
| Metric | executor_calls / duplicate_executions on double deliver |
| Observed value | `executor_calls=1`, `duplicate_executions=1`, `processed_messages=2` |
| Benchmark command | `./worker/scripts/smoke_kafka_execution_replay.sh` |
| Report path | `docs/benchmarks/day114-kafka-execution-replay.md` |
| Limitations | Functional smoke, not a large-N duplicate **rate** study; crash-after-claim gap remains |
| Safe to use on resume? | **Yes** as “duplicate delivery suppressed to one execution (smoke)” |
| Label | **Verified** |

### Resilience recovery time

| Field | Value |
|-------|--------|
| Metric | Observed Kafka restore time (compose stop/start) |
| Observed value | Example Day 129 log: `observed_recovery_ms≈8193` (machine-specific) |
| Report path | `docs/benchmarks/day129-resilience.md` |
| Limitations | Not MTTR vs a prior procedure; not production |
| Safe to use on resume? | **Qualitative only** |
| Label | **Verified** (observation) / **Unsupported** as “60% MTTR reduction” |

## Target bullets — Day 130 disposition

### Bullet 1 — “3.1× throughput” + p95 under burst

| Claim piece | Disposition |
|-------------|-------------|
| 3.1× sustained throughput | **Unsupported** — no committed baseline/optimized pair documenting 3.1× under equivalent workload |
| Weighted-fair + Go pool + backpressure | **Verified** (implementation + tests) |
| Stabilizing p95 under burst | **Requires new experiment** — queue-wait p95 plumbing exists; burst p95 comparison study not committed |

**Safe alternative:** “Implemented a weighted-fair Python scheduler and concurrent Go worker pool with bounded queues and backpressure; measured local scheduler claim throughput (~24k jobs/sec avg at 1k×3 on Docker Postgres) and full-path E2E completion benchmarks (local only).”

### Bullet 2 — “0.01% duplicates” + “99.95% successful completions”

| Claim piece | Disposition |
|-------------|-------------|
| 0.01% duplicate execution rate | **Unsupported** — N≫10k rate experiment not committed; smoke shows 1/2 deliveries skipped |
| 99.95% successful completions | **Unsupported** — Day 94 shows 10/10 local; insufficient volume |
| Idempotent state + Redis + Kafka replay | **Verified** |
| Crash-after-claim gap | Must **acknowledge** — see known-limitations |

**Safe alternative:** “Implemented Redis-backed idempotency across dispatch, execution, and result paths; Kafka duplicate-delivery smoke confirms one business execution per attempt (`executor_calls=1` on double publish), with the crash-after-claim recovery gap documented.”

### Bullet 3 — “60% MTTR” + CloudWatch alerts

| Claim piece | Disposition |
|-------------|-------------|
| 60% MTTR reduction | **Unsupported** — no before/after timed recovery study |
| OpenTelemetry + Prometheus + runbooks | **Verified** |
| CloudWatch alerts wired and delivering | **Unsupported** — offline CloudWatch-oriented config only |

**Safe alternative:** “Instrumented gRPC/Kafka/execute paths with OpenTelemetry, exported Prometheus metrics, and authored alert rules plus incident runbooks for queue depth, worker availability, and dependency outages (local validation; CloudWatch delivery not claimed).”

## Experiments required for original numbers

| Target number | Required experiment |
|---------------|---------------------|
| 3.1× throughput | Fixed workload A vs B (fairness+pool on/off or FIFO vs weighted) with identical job mix; report throughput + p95 |
| Burst p95 stability | Controlled burst generator; record p95 execute/queue latency before/after backpressure |
| 0.01% duplicate rate | ≥10⁵ deliveries with intentional replay fraction; `(duplicate_completions)/(executions)` |
| 99.95% success | Large-N E2E with defined denominator and failure injection policy |
| 60% MTTR | Timed incident drills with/without runbooks+telemetry; same fault class |
