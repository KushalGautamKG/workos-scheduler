# Runbooks

## MVP Operational Smoke Tests

Quick checks from the **repository root** after infra is up (`docker compose up -d postgres zookeeper kafka`, `./infra/kafka/create-topics.sh`). See **[mvp.md](mvp.md)** for full MVP context. **[Day 90 checkpoint](checkpoints/day90.md)** summarizes production-readiness state, completed features, remaining gaps, and roadmap toward **Redis**, **gRPC**, **OpenTelemetry**, **Kubernetes/EKS**, and **CloudWatch**.

| Path | Command |
|------|---------|
| **Success** | `./control_plane/scripts/smoke_full_completion.sh` |
| **Retry** | `./control_plane/scripts/smoke_retry_requeue.sh` |
| **Exhaustion** | `./control_plane/scripts/smoke_retry_exhaustion.sh` |
| **Queue wait metrics** | `./control_plane/scripts/smoke_queue_wait_metrics.sh` |
| **Worker queue saturation** | `./worker/scripts/smoke_queue_saturation.sh` |
| **Worker backpressure config** | `./worker/scripts/smoke_backpressure_config.sh` |
| **Dead-letter inspection** | `PYTHONPATH=. python3 control_plane/scripts/list_dead_lettered_jobs.py` |
| **Manual recovery** | `PYTHONPATH=. python3 control_plane/scripts/requeue_dead_lettered_job.py <job_id>` |

After manual requeue, run **`PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py`** to dispatch. All smoke scripts exit nonzero on failure.

Each MVP smoke script prints a final **`event=smoke_*`** summary line (`success=true|false`, plus `job_id` and state fields). Redirect output to a file, then:

```bash
grep "event=smoke_" run.log
```

Lines with **`success=false`** include an **`error=`** field — read that before scrolling the full log.

## Reading Structured Script Logs

One-shot control-plane scripts print a final **key=value** summary line (Python: `control_plane/kernelq/logging_utils.py`; smoke tests: bash helpers in `scripts/smoke_*.sh`). When reviewing script output or saved logs, **start with `event=<name>`** to find the script’s outcome in one line.

| `event` | What to check |
|---------|----------------|
| `smoke_full_completion` | `success`, `final_state=succeeded`, `job_id` |
| `smoke_retry_requeue` | `success`, `state_after_retry_result`, `state_after_retry_scanner`, `state_after_scheduler_tick` |
| `smoke_retry_exhaustion` | `success`, `final_state=dead_lettered`, `state_after_retry_scanner=dead_lettered` |
| `smoke_queue_wait_metrics` | `success`, `queue_wait_seconds` > 0, `job_id` |
| `smoke_worker_queue_saturation` | `success=true` — bounded-queue backpressure boundary (no Kafka); test expects **`work_queue_full_errors > 0`** |
| `smoke_worker_backpressure_config` | `success=true` — **`cmd/consumer`** startup logs **`backpressure_enabled`**, **`backpressure_high_ratio`**, **`backpressure_low_ratio`** |
| `scheduler_tick` | `published_count`, `errors_count`, `publish_errors_count` |
| `retry_scanner` | `requeued_count`, `errors_count`, optional `requeued_job_ids` |
| `result_consumer` | `processed_message`, `errors_count`, optional `error` |
| `job_state_snapshot` | `total_jobs`, `states_count` |
| `job_duration_snapshot` | `completed_jobs_count`, averages, **`p50_queue_wait_seconds`**, **`p95_queue_wait_seconds`**, **`p99_queue_wait_seconds`** |
| `seed_latency_metrics` | `created_jobs` |
| `generate_load_jobs` | `created_jobs`, `elapsed_seconds`, `jobs_per_second`, `tenants` |
| `benchmark_scheduler_throughput` | `trials`, `generated_jobs_per_trial`, `min_jobs_dispatched_per_second`, `avg_jobs_dispatched_per_second`, `max_jobs_dispatched_per_second`, `total_dispatched_jobs` |
| `benchmark_worker_throughput` | Single trial: `generated_jobs`, `processed_jobs`, `elapsed_seconds`, `jobs_processed_per_second`, `worker_count`, `queue_capacity`. Multi-trial: `trials`, `generated_jobs_per_trial`, `total_processed_jobs`, `min_jobs_processed_per_second`, `avg_jobs_processed_per_second`, `max_jobs_processed_per_second` |
| `benchmark_end_to_end_completion` | `generated_jobs`, `dispatched_jobs`, `succeeded_jobs`, `elapsed_seconds`, `jobs_completed_per_second`, `worker_count`, `queue_capacity`, `job_prefix` |

**Duration metrics:** Queue wait **p50/p95/p99** from **`dispatched_at - created_at`**. JSON: **`GET /metrics/durations`**; Prometheus gauges: **`GET /metrics/prometheus`**. Snapshot quantiles — **not histogram `_bucket` metrics yet**. **`seed_latency_metrics.py`** seeds realistic queue waits for local testing; **`smoke_queue_wait_metrics.sh`** verifies non-zero queue latency end-to-end.

**Load generator:** **`generate_load_jobs.py`** creates **`queued`** benchmark rows in Postgres (use a unique **`--prefix`** for cleanup and reproducibility). Dispatch with **`run_scheduler_tick_once.py`** when ready.

## Performance / Benchmarking

Use **`benchmark_scheduler_throughput.py`** to measure scheduler dispatch throughput. **`--trials`** repeats runs with a unique prefix per trial and reports **min/avg/max** **`jobs_dispatched_per_second`** — use repeated trials for more credible local numbers. Still **local baselines**, not production capacity claims (`event=benchmark_scheduler_throughput`).

**Worker throughput:** **`./worker/scripts/benchmark_worker_throughput.sh`** — prefix-isolated dispatch → worker → result; **`TRIALS`** for repeated runs (**min/avg/max** when **`TRIALS>1`**). **Local dev only — not production capacity.** See **[benchmarks/day91-worker-throughput.md](benchmarks/day91-worker-throughput.md)**.

**End-to-end completion (Day 94):** **`./control_plane/scripts/benchmark_end_to_end_completion.sh`** — **`queued` → `dispatched` → worker result → `succeeded`**. Complements scheduler and worker benchmarks. **Local dev only.** See **[benchmarks/day94-end-to-end-completion.md](benchmarks/day94-end-to-end-completion.md)**.

**Go worker pool:** **`cmd/consumer`** uses a **worker pool** (default **4** workers) with a **bounded work queue** (default **100** slots). When full: log **`event=worker_queue_full`**, increment **`work_queue_full_errors`**, then **Day 82 local backoff** (**50ms**, **one retry**) to **reduce enqueue pressure during bursts**—poll loop continues. **`smoke_queue_saturation.sh`** validates saturation (no Kafka).

**Worker saturation**

- **Symptoms:** **`work_queue_full_errors`** rising; high **`work_queue_depth`** vs **`work_queue_capacity`**; grep **`event=worker_queue_full`**, **`event=worker_backpressure_pause`**, **`event=worker_backpressure_resume`**.
- **Current mitigation:** Inspect **`work_queue_full_errors`**, **`backpressure_pause_events`** / **`backpressure_resume_events`**, and worker capacity—tune **`KERNELQ_WORKER_COUNT`** / **`KERNELQ_WORKER_QUEUE_CAPACITY`**; run **`./worker/scripts/smoke_queue_saturation.sh`**.
- **Day 88 config:** **`KERNELQ_WORKER_BACKPRESSURE_ENABLED`** (default **`false`**), **`KERNELQ_WORKER_BACKPRESSURE_HIGH_RATIO`**, **`KERNELQ_WORKER_BACKPRESSURE_LOW_RATIO`**—runtime watermark tuning; prepares for **Kubernetes/EKS ConfigMaps** later. **`./worker/scripts/smoke_backpressure_config.sh`** validates startup output. See **`worker/README.md`**.
- **Day 87–88 behavior:** when enabled, worker evaluates queue depth vs watermarks; increments pause/resume counters—**in-memory controller only**; **real Kafka partition pause/resume** still future ([`docs/design/kafka-pause-resume-backpressure.md`](design/kafka-pause-resume-backpressure.md)).

If **`dispatched_jobs` < `generated_jobs`**, inspect:

- Postgres connectivity
- Scheduler tick errors (lines above the summary)
- Kafka / no-op producer behavior (benchmark uses **`job_producer=None`**; Kafka publish failures apply only if you changed that)
- Queued job state (rows still **`queued`**? higher-priority competitors in a shared DB?)

**Key fields (across events):**

- **`errors_count`** — non-zero means the pass failed or hit errors; investigate human-readable lines above the summary.
- **`job_id`** — when present, ties the line to one job (grep by id during distributed debugging).
- **`requeued_count`** — how many due retries moved to `queued`.
- **`processed_message`** — `true` if the result consumer got and handled a Kafka message this poll.
- **`published_count`** — how many dispatch events reached Kafka after a scheduler tick.
- **`success`** — on smoke lines, `true` means the MVP path passed; `false` means check **`error=`** on the same line.

**Grep examples** (redirect script output to a file first, or search CI logs):

```bash
grep "event=smoke_" run.log
grep "success=false" run.log
grep "event=retry_scanner" run.log
grep "event=scheduler_tick" run.log | grep "publish_errors_count=0"
grep "job_id=day57-smoke" run.log
```

These lines are **grep-friendly** for local ops; KernelQ does not ship centralized log aggregation yet.

## High Queue Depth

**Symptoms:**
- Queue depth metric shows thousands of pending jobs
- Worker utilization is at 100%
- New jobs are taking longer to start

**Checks:**
- Check worker count and health status
- Verify broker is processing messages
- Review job execution times (are jobs stuck?)
- Check for resource limits (CPU, memory, concurrency)

**Mitigation:**
- Scale up workers if resources allow
- Check for stuck or slow-running jobs and kill if needed
- Temporarily pause new job enqueuing if system is overwhelmed
- Review and adjust concurrency limits

**Follow-up:**
- Analyze root cause (sudden spike? slow jobs? worker crash?)
- Update capacity planning
- Consider implementing backpressure mechanisms

## P95 Latency Spike

**Symptoms:**
- P95 latency jumps from normal to 10x+ baseline
- Some jobs complete quickly, others are very slow
- User complaints about slow job execution

**Checks:**
- Check database query performance
- Review broker message processing rate
- Check for network issues between components
- Look for specific job types causing slowdowns
- Check worker resource utilization

**Mitigation:**
- Identify and kill slow-running jobs if safe
- Check database indexes and query plans
- Verify broker is not backlogged
- Restart workers if they appear stuck
- Temporarily reduce concurrency to reduce contention

**Follow-up:**
- Profile slow jobs to find bottlenecks
- Optimize database queries or add indexes
- Review and tune worker concurrency settings
- Add more granular latency metrics

## Broker Down

**Symptoms:**
- Workers report "connection refused" or "broker unavailable"
- No jobs are being consumed from queue
- Control plane cannot enqueue new jobs
- Queue depth growing but not decreasing

**Checks:**
- Verify broker process is running
- Check broker health endpoint
- Review broker logs for errors
- Check network connectivity to broker

**Mitigation:**
- Restart broker service
- If broker is on separate host, check host health
- Failover to backup broker if available
- Temporarily pause job enqueuing to prevent queue buildup

**Follow-up:**
- Investigate root cause of broker failure
- Review broker configuration and resource limits
- Consider broker high-availability setup
- Test failover procedures

## Database Slow

**Symptoms:**
- Database query latency spikes
- Control plane API responses are slow
- Workers report slow state updates
- Database connection pool exhausted

**Checks:**
- Check database CPU and memory usage
- Review slow query log
- Check for long-running transactions
- Verify database connection pool settings
- Check for table locks or deadlocks

**Mitigation:**
- Kill long-running queries if safe
- Restart database connections
- Scale up database resources if possible
- Temporarily reduce write frequency
- Enable read replicas if available

**Follow-up:**
- Analyze slow queries and optimize
- Review database indexes
- Consider connection pooling improvements
- Plan database scaling strategy

## Worker Crash Loop

**Symptoms:**
- Workers restarting repeatedly
- High error rate in worker logs
- Jobs failing immediately after starting
- Worker process exits with errors

**Checks:**
- Review worker logs for crash reason
- Check worker resource limits (memory, CPU)
- Verify worker configuration is valid
- Check for dependency failures (broker, database)
- Review recent code deployments

**Mitigation:**
- Stop crashing workers to prevent resource drain
- Roll back recent code changes if applicable
- Fix configuration errors
- Restart with increased resource limits if OOM
- Check for dependency service outages

**Follow-up:**
- Fix root cause (code bug, config error, resource issue)
- Add better error handling and graceful degradation
- Improve worker health checks
- Review monitoring and alerting

## Worker Encounters Invalid Kafka Message

**Symptoms:**
- Worker logs parse or validation errors (malformed JSON, wrong `event_type`, blank `job_id`, invalid `state`)
- Shutdown stats show **`message_errors`** increasing while the process stays up

**Immediate impact:**
- The worker **skips the bad record** and **continues polling**—it does **not** exit on a single bad dispatch message
- Healthy messages on **`kernelq.jobs.dispatch`** can still be processed

**Current behavior:**
- Invalid messages increment **`MessageErrors`** and the worker **keeps polling**
- When **`DeadLetterProducer`** is configured (wired in **`cmd/consumer`**), failures are routed to **`kernelq.jobs.dlq`** as **`DeadLetterEvent`** JSON (`reason`, `original_key`, `original_value`, `source_topic`, `worker`)
- **`DeadLettersPublished`** increases on successful DLQ publish; **`DeadLetterPublishErrors`** increases if publish fails
- **`kafka.Error`** (broker/connection failures) still **stops** the worker

**What operators should inspect:**
- Consume **`kernelq.jobs.dlq`** to read **`reason`** and **`original_value`** (raw dispatch payload as received)
- **`original_key`** — often `job_id` when the producer set a key
- Shutdown stats from **`cmd/consumer`**: `message_errors`; **`DeadLettersPublished`** / **`DeadLetterPublishErrors`** in **`ConsumerStats`**
- Compare payload to **`DispatchEvent`** in `worker/internal/worker/dispatch_event.go`

**Inspect DLQ on Kafka (local):**

```bash
docker exec -i kernelq-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic kernelq.jobs.dlq \
  --from-beginning \
  --max-messages 5
```

**Common causes:**
- **Schema drift** — Python publish fields or values no longer match Go validation
- **Manual test messages** — ad hoc JSON on `kernelq.jobs.dispatch` (console producer, old smoke tests)
- **Corrupt producer payloads** — truncated JSON, wrong `event_type`, blank ids, invalid `state`

**Checks:**
- Read the failed record from **`kernelq.jobs.dlq`** (preferred) or the original offset on **`kernelq.jobs.dispatch`**
- Compare JSON to the dispatch contract; check shutdown stats (`message_errors`, **`DeadLetterPublishErrors`**)
- If **`DeadLetterPublishErrors`** rises, DLQ routing failed—check **Kafka producer connectivity** (`localhost:9092` from worker host) and confirm **`kernelq.jobs.dlq`** exists (`./infra/kafka/create-topics.sh`)
- Search topic history for non-production traffic after incidents

**Follow-up:**
- Fix the publisher or remove bad records on the topic if safe in dev
- Align control-plane publish validation with worker rules
- Monitor DLQ depth; replay only after fixing root cause

## Worker Reports Retryable Failure

**What it means:**

A **`retryable_failure`** **`ExecutionResult`** means the job attempt failed, but **retrying later may succeed**. These are **temporary execution issues**—not poison Kafka messages and not permanent job failures.

**Examples:**

- **Transient dependency outage** — downstream API or database briefly unavailable
- **Temporary network issue** — connection reset or timeout between worker and dependency
- **Rate limiting** — upstream returned 429 or throttled the worker; backoff may help

**Current behavior:**

- Workers classify this outcome in **`ExecutionResult`** (`status: retryable_failure`, optional **`message`**).
- **Automatic retry workflows are not wired yet**—no publish to **`kernelq.jobs.retry`**, no Postgres **`failed → retry_scheduled`** from worker reports today.

**Future behavior:**

- Scheduler/retry path will use **`retryable_failure`** to trigger **`failed → retry_scheduled → queued`**, honor **`retry_count` / `max_retries`**, and re-publish to **`kernelq.jobs.retry`**.

**What operators should do today:**

- Treat rising retryable failures as **dependency or capacity signals**—check downstream health, network, and rate limits before jobs stall or exhaust retries.
- Distinguish from **invalid dispatch messages** (DLQ on **`kernelq.jobs.dlq`**) and **terminal failures** (no auto-retry).

## Worker Result Event Missing

**Symptoms:**

- Job stays **`dispatched`** or **`running`** in Postgres with **no terminal update** (`succeeded`, `failed`, `dead_lettered`, `canceled`)
- Dispatch message was consumed but lifecycle never closed the loop

**Possible causes:**

- **Worker crashed before publishing result** — execution started or finished in-process but no **`WorkerResultEvent`** reached **`kernelq.jobs.results`**
- **Result topic unavailable** — **`kernelq.jobs.results`** missing or broker unreachable from worker (`./infra/kafka/create-topics.sh`)
- **Result consumer not running** — Python control plane is not yet consuming results (expected today)
- **Invalid result event rejected** — malformed JSON or bad **`status`** / **`event_type`** dropped by future consumer validation

**Checks:**

- Run **`./worker/scripts/smoke_worker_result.sh`** (from repo root) to verify worker-side result publishing end to end
- Run **`PYTHONPATH=. python3 control_plane/scripts/consume_result_once.py`** to poll **one** result message and update Postgres
  - **`poll_result: processed_message=false`** — no message on **`kernelq.jobs.results`** before the timeout (produce a result first, or increase wait)
  - **Message processed but state unchanged** — check **`job_id` exists in Postgres** and **`ResultStateHandler`** mapping (**`succeeded`** → **`succeeded`**; failures → **`failed`** today)
- If the smoke test fails, inspect:
  - **Worker logs** — `/tmp/kernelq-worker-smoke.log` (script output) or your running consumer process
  - **`kernelq.jobs.dispatch`** — dispatch message present and valid JSON
  - **`kernelq.jobs.results`** — result event with matching **`job_id`**
  - **Kafka connectivity** — broker up (`docker compose up -d kafka zookeeper`), topics exist (`./infra/kafka/create-topics.sh`), worker reaches **`localhost:9092`**
- Confirm worker process was up when the job was dispatched
- Inspect **`kernelq.jobs.results`** for a record with matching **`job_id`**
- If result events are missing, check:
  - **`ResultProducer` wiring** — **`cmd/consumer`** passes **`KafkaResultProducer`** into **`DispatchEventHandler`**
  - **`kernelq.jobs.results` topic exists** — `./infra/kafka/create-topics.sh`
  - **Kafka producer connectivity** — worker can reach broker (`localhost:9092`)
  - **Handler publish errors** — **`PublishResult`** failure returns error from **`Handle`** (may increment **`message_errors`**)
- Compare worker logs and shutdown stats with **`kernelq.jobs.dispatch`** / DLQ traffic

**Current status:**

- **Worker handler publishes result events** when **`ResultProducer`** is configured (**`DispatchEventHandler`** → **`PublishResult`** after **`Execute`**)
- **`KafkaResultProducer`** wired in **`cmd/consumer`** with **`WorkerName: kernelq-go-worker`**
- **Python result event parser exists** — **`control_plane/kernelq/result_event.py`** validates **`WorkerResultEvent`** JSON (including allowed **`status`** values)
- **`ResultConsumerRunner` exists** (`control_plane/kernelq/result_consumer.py`) — parses raw result bytes and delegates to a **`ResultHandler`**
- **`ResultStateHandler` exists** (`control_plane/kernelq/result_handler.py`) — maps **`status`** → **`jobs.state`**
- **`KafkaResultConsumer` exists** (`control_plane/kernelq/kafka_result_consumer.py`) — **`poll_once`** on **`kernelq.jobs.results`**; manual script **`consume_result_once.py`**
- **Long-running result consumer loop** is not implemented yet
- **`retryable_failure`** and **`terminal_failure`** both map to **`failed`** (**FAILED**) today — **retry scheduling** (`RETRY_SCHEDULED`, **`DEAD_LETTERED`**) is not implemented yet

**Follow-up (when result pipeline lands):**

- Alert on jobs stuck in **`dispatched`** / **`running`** past SLA
- Monitor result-topic lag and consumer errors alongside dispatch lag

## Result Event Consumed but Job State Unchanged

Use this when a **`WorkerResultEvent`** was parsed/handled but **`jobs.state`** in Postgres did not move.

**Check:**

- **`job_id` exists in Postgres** — **`ResultStateHandler`** raises if **`update_job_state_from_worker_result`** returns **`False`**
- **Repository update returned `True`** — confirm the row was found and updated (not a silent no-op)
- **`status` mapping is supported** — today: **`succeeded`** → **`succeeded`**; **`retryable_failure`** / **`terminal_failure`** → **`failed`**
- **Result consumer is wired to the handler** — **`ResultConsumerRunner`** must use **`ResultStateHandler(repository)`**, not a no-op fake (Kafka subscribe/poll still future work for production)

**Note:** **`retryable_failure`** uses **`schedule_retry_from_worker_result`** — see **Retryable Worker Failure** below. **`terminal_failure`** still maps to **`failed`** today.

## Retryable Worker Failure

Use this when a consumed **`WorkerResultEvent`** has **`status: retryable_failure`** (transient execution failure; another attempt may succeed).

**Symptom:**

- Result on **`kernelq.jobs.results`** shows **`retryable_failure`** for a **`job_id`**
- After **`ResultStateHandler`** runs, Postgres **`retry_count`** or **`state`** changed (or job is **`dead_lettered`** if exhausted — see **Job Reaches DEAD_LETTERED**)

**Current behavior (control plane):**

- **`ResultStateHandler`** calls **`JobRepository.schedule_retry_from_worker_result`**
- If **`retry_count < max_retries`**: **`retry_count`** increments by 1, job moves to **`retry_scheduled`**
- If retries are **exhausted**: job moves to **`dead_lettered`** (terminal — see **Job Reaches DEAD_LETTERED**)
- **No automatic re-run** while **`retry_scheduled`** — run **`run_retry_scanner_once.py`** after **`retry_after`** is due

**Future behavior:**

- **Retry delay / backoff** while in **`retry_scheduled`**
- **Long-running automatic requeue** — continuous scanner daemon (today: manual one-shot script)

**Checks:**

- Confirm result was consumed (`consume_result_once.py` or result consumer)
- Query Postgres: `SELECT job_id, state, retry_count, max_retries, retry_after FROM jobs WHERE job_id = '<id>';`
- If still **`retry_scheduled`**, see **Jobs Stuck in RETRY_SCHEDULED** below

## Jobs Stuck in RETRY_SCHEDULED

Use this when a job stays **`retry_scheduled`** and never returns to **`queued`** / gets redispatched.

**Symptoms:**

- **`jobs.state = retry_scheduled`** for longer than expected
- Scheduler does not pick the job (only **`queued`** rows are schedulable)

**Checks:**

1. **Inspect `retry_after`** (Unix seconds):
   ```bash
   docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
     -c "SELECT job_id, state, retry_after, retry_count, max_retries FROM jobs WHERE job_id = '<id>';"
   ```
2. **If `retry_after` is in the future** — the job is **waiting correctly**; run the scanner again after that time (no long-running loop yet).
3. **If `retry_after` is in the past** — run the one-shot requeue scanner from repo root:
   ```bash
   PYTHONPATH=. python3 control_plane/scripts/run_retry_scanner_once.py
   ```
   Expect **`requeued_job_ids`** to include your **`job_id`** and state → **`queued`**. Then run **`run_scheduler_tick_once.py`** to dispatch.
4. **If `retry_after` is past but the scanner does not requeue** — inspect **`errors`** in script output and Postgres connectivity; confirm **`retry_after`** column exists on **`jobs`**.
5. **Verify the full retry requeue path** (repo root):
   ```bash
   ./control_plane/scripts/smoke_retry_requeue.sh
   ```
   Expect **`retry_scheduled` → `queued` → `dispatched`**. If it fails, inspect **`retry_after`**, **`run_retry_scanner_once.py`** output (**`requeued_job_ids`**, **`errors`**), **`run_scheduler_tick_once.py`** output (**`dispatched_job_ids`**, **`publish_errors`**), and Postgres **`state`** / **`retry_count`** after each step.

**Note:** If the job is **`dead_lettered`** instead, retries are exhausted — see **Job Reaches DEAD_LETTERED**.

## Job Reaches DEAD_LETTERED

Use this when **`jobs.state = dead_lettered`** after a worker result or retry exhaustion.

**Meaning:**

- **Retry budget exhausted** — **`retry_count >= max_retries`** after a **`retryable_failure`**
- **Permanent failure** (policy) — **`terminal_failure`** may map to **`failed`** today; **`dead_lettered`** is the target for non-retryable outcomes

**Immediate impact:**

- **`DEAD_LETTERED` is terminal** — the job **will not be retried automatically**
- **`RetryScanner` must not requeue** **`dead_lettered`** rows (only **`retry_scheduled`** with due **`retry_after`** → **`queued`**). If a dead-lettered job reappears in **`queued`**, that is a policy bug.

**Verify exhaustion behavior (local):**

```bash
./control_plane/scripts/smoke_retry_exhaustion.sh
```

Expect **`final_state=dead_lettered`** and **`state_after_retry_scanner=dead_lettered`**.

**List dead-lettered jobs (read-only):**

```bash
PYTHONPATH=. python3 control_plane/scripts/list_dead_lettered_jobs.py
```

Shows up to 20 recent **`dead_lettered`** rows (newest **`updated_at`** first). Inspect **`payload`**, **`retry_count`/`max_retries`**, and **`updated_at`** per job. Read-only.

**Manual recovery:**

1. **Inspect** the job (and others) with **`list_dead_lettered_jobs.py`** — confirm **`job_id`**, payload, and retry budget.
2. **Fix external cause** if needed (dependency, payload, worker bug, config).
3. **Requeue** — only **`DEAD_LETTERED`** jobs are eligible; **`retry_count`** is **preserved** for audit/history:
   ```bash
   PYTHONPATH=. python3 control_plane/scripts/requeue_dead_lettered_job.py <job_id>
   ```
   Expect **`requeued job_id=<job_id> state=queued`**. Exits nonzero if the job is missing or not dead-lettered.
4. **Dispatch** — run a scheduler tick or wait for the scheduler to pick up **`queued`** jobs:
   ```bash
   PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py
   ```

**Checks:**

1. **Inspect `retry_count` and `max_retries`** — confirm exhaustion vs misconfiguration:
   ```bash
   docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
     -c "SELECT job_id, state, retry_count, max_retries, retry_after, updated_at FROM jobs WHERE job_id = '<id>';"
   ```
   Expect **`retry_count >= max_retries`** when dead-lettered from **`retryable_failure`**.
2. **Worker result message** — find the event on **`kernelq.jobs.results`** (matching **`job_id`**, **`status`**, **`message`**):
   ```bash
   docker exec kernelq-kafka kafka-console-consumer \
     --bootstrap-server kafka:29092 \
     --topic kernelq.jobs.results \
     --from-beginning \
     --timeout-ms 5000 \
     --max-messages 500 2>/dev/null | grep -F "<job_id>"
   ```
   Or re-run **`PYTHONPATH=. python3 control_plane/scripts/consume_result_once.py`** if the result is still on the topic.
3. **Job payload** — inspect the original job row (payload, tenant, priority) for poison input or bad config:
   ```bash
   docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
     -c "SELECT job_id, tenant_id, priority, payload, state, created_at FROM jobs WHERE job_id = '<id>';"
   ```

**Mitigation (today):**

- Follow **Manual recovery** above — do not requeue until root cause is understood
- In dev, you may still create a **new job** instead of requeueing the dead-lettered row

**Future improvement:**

- **DLQ inspection** — consume **`kernelq.jobs.dlq`**, dashboards, alerting on **`dead_lettered_jobs_total`**

## Full Completion Smoke Test Fails

Use this when **`./control_plane/scripts/smoke_full_completion.sh`** exits nonzero or **`final_state`** is not **`succeeded`**.

**Symptoms:**

- Script prints **`FAIL: expected final_state=succeeded`**
- **`final_state=dispatched`**, **`running`**, **`failed`**, or **`missing`** after the result wait loop
- **`consume_result_once.py`** traceback (**`job not found`**, parse error, Kafka error)
- Worker log shows **`message_errors`** or no **`received task`** line for the smoke **`job_id`**

**Checks (in order):**

1. **Postgres running**
   - `docker compose up -d postgres`
   - `docker exec kernelq-postgres pg_isready -U kernelq -d kernelq`
   - Migration applied: `docker exec -i kernelq-postgres psql -U kernelq -d kernelq < control_plane/migrations/001_create_jobs.sql`

2. **Kafka / Zookeeper running**
   - `docker compose up -d zookeeper kafka`
   - `docker compose ps` — both containers healthy

3. **Topics exist**
   - `./infra/kafka/create-topics.sh`
   - Confirm **`kernelq.jobs.dispatch`** and **`kernelq.jobs.results`** appear in the list

4. **Worker starts**
   - Script builds **`./worker/consumer`** and starts it in the background
   - Inspect **`/tmp/kernelq-full-worker.log`** — expect **`KernelQ worker consumer started`**
   - If the worker exits immediately, check Go build errors and **`localhost:9092`** reachability

5. **Scheduler tick publishes dispatch**
   - Re-run: `PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py`
   - Expect **`dispatched_count: 1`**, **`published_count: 1`**, and your smoke **`job_id`** under **`dispatched_job_ids`**
   - If **`(none)`**, confirm a **`queued`** row exists for the smoke **`job_id`** (script creates one; re-runs need a fresh job)

6. **Result topic receives event**
   - After dispatch, grep the results topic for the smoke **`job_id`** (local):
     ```bash
     docker exec kernelq-kafka kafka-console-consumer \
       --bootstrap-server kafka:29092 \
       --topic kernelq.jobs.results \
       --from-beginning \
       --timeout-ms 5000 \
       --max-messages 500 2>/dev/null | grep -F "<job_id>"
     ```
   - Or run **`./worker/scripts/smoke_worker_result.sh`** to isolate worker → result publishing

7. **`consume_result_once.py` processed matching job**
   - `PYTHONPATH=. python3 control_plane/scripts/consume_result_once.py`
   - **`poll_result: processed_message=true`** — a message was read (may not be *your* job yet)
   - **`ValueError: job not found`** — stale result for a **`job_id`** not in Postgres; see stale-message note below
   - Full smoke script retries in a loop until the target **`job_id`** reaches **`succeeded`**

8. **Final Postgres state is SUCCEEDED**
   - Script prints **`final_state=succeeded`** on success
   - Manual check:
     ```bash
     docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
       -c "SELECT job_id, state FROM jobs WHERE job_id LIKE 'day52-full-%' ORDER BY created_at DESC LIMIT 5;"
     ```

**Stale Kafka messages:**

- **`kernelq.jobs.results`** retains old smoke-test events (e.g. **`day47-smoke-*`**) with **no matching Postgres row**
- **`consume_result_once.py`** polls **one message at a time**; older records may be consumed first and fail or update unrelated jobs
- The full completion script uses a **unique `job_id`** (`day52-full-<timestamp>`) and **retries** consumption until **that** row is **`succeeded`**
- If failures persist, drain stale traffic in dev (new consumer group, topic reset—dev only) or keep retrying until offsets pass old messages

**Mitigation:**

- Re-run from repo root: `./control_plane/scripts/smoke_full_completion.sh` (creates a fresh job each time)
- Increase wait time in the script if the worker is slow to start (default includes sleep after worker launch)
- Fix the first failing step in the checklist above before debugging later steps

**Follow-up:**

- If dispatch works but results never appear, use **Worker Result Event Missing** checks above
- If results are consumed but state stays wrong, use **Result Event Consumed but Job State Unchanged**
- Retry scheduling and DLQ behavior are **not** part of this smoke test yet—failures map to **`failed`** only today
