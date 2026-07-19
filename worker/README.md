# KernelQ Worker Plane (Go)

This folder is the **Go worker plane** for KernelQ—the half of the system that **executes** jobs after the **Python control plane** decides they are ready to run.

## What workers will do (later)

Today the control plane **schedules** work: jobs live in Postgres, scheduler ticks claim them, and **dispatch events** are published to Kafka topic **`kernelq.jobs.dispatch`**.

**Go workers will eventually:**

1. **Consume dispatch events** from Kafka (same JSON shape as Python `DispatchEvent`: `job_id`, `tenant_id`, `priority`, `payload`, …).
2. **Validate and run** each job using bounded concurrency—only *N* jobs at a time per worker process so memory and CPU stay predictable under load.
3. **Report state back** to Postgres (`running`, `succeeded`, `failed`, etc.).

Kafka carries the handoff; Postgres stays the system of record. See `docs/architecture.md` and `docs/decisions/ADR-0001-foundations-and-language-split.md`.

**Day 113 — execution idempotency smoke:** **`./worker/scripts/smoke_worker_execution_idempotency.sh`** — live Redis + real handler; same `job_id`+`attempt` twice → executor once, second `duplicate_skipped`. No Kafka.

**Day 114 — Kafka execution replay:** **`./worker/scripts/smoke_kafka_execution_replay.sh`** — same dispatch event published twice on Kafka; Redis claim skips second execution (`executor_calls=1`, `duplicate_executions=1`, `processed_messages=2`). Duplicate replay is expected, not an error.

**Day 115 — claim-before-completion gap:** **`./worker/scripts/smoke_execution_claim_gap.sh`** — educational: claim Redis execution key, skip execute, claim again → `recovery_needed=true`. Duplicate suppression is complete; **execution recovery intentionally deferred** (lease + watchdog). See **[execution-recovery.md](../docs/design/execution-recovery.md)**.

**Day 116 — internal gRPC execution service:** **`proto/worker_execution.proto`** + **`internal/grpc`** skeleton. Kafka remains the dispatch mechanism.

**Day 117 — gRPC network listener:** **`cmd/grpc-server`** on **`KERNELQ_GRPC_ADDR`** (default `127.0.0.1:50051`) + client + **`./worker/scripts/smoke_grpc_execute.sh`**. Reuses **`DispatchEventHandler`**. Localhost validation complete; **no production RPC routing yet**. See **[grpc-worker-execution.md](../docs/design/grpc-worker-execution.md)**.

## What exists today

This is **foundation only**—not a running worker yet:

| Path | Purpose |
|------|---------|
| `go.mod` | Go module (`github.com/KushalGautamKG/workos-scheduler/worker`, Go 1.22) |
| `internal/worker/task.go` | `Task` struct and `ValidateTask()` |
| `internal/worker/task_test.go` | Unit tests for task validation |
| `internal/worker/dispatch_event.go` | `DispatchEvent`, `ParseDispatchEvent` |
| `internal/worker/consumer.go` | `ConsumerRunner`, `ProcessMessage` |
| `internal/worker/handler.go` | `DispatchEventHandler` |
| `internal/worker/idempotency.go` | `IdempotencyStore` interface (`TryClaim`) |
| `internal/worker/in_memory_idempotency.go` | `InMemoryIdempotencyStore` (tests; no Redis) |
| `internal/worker/redis_idempotency.go` | `RedisIdempotencyStore` + go-redis `SetNX` adapter |
| `cmd/smoke_redis_idempotency` | Live Redis smoke (`SET NX EX`) |
| `cmd/idempotency-smoke` | Live Redis handler execution-dedupe smoke (Day 113) |
| `scripts/smoke_kafka_execution_replay.sh` | Kafka duplicate-dispatch replay + Redis execution claim (Day 114) |
| `scripts/smoke_execution_claim_gap.sh` | Educational claim-before-completion gap demo (Day 115; no fix) |
| `internal/grpc` | `WorkerExecutionService` server + client (Days 116–117) |
| `cmd/grpc-server` | Localhost gRPC listener (Day 117; graceful shutdown) |
| `cmd/grpc-execute` | Thin CLI client for loopback smoke |
| `scripts/smoke_grpc_execute.sh` | Loopback SUCCESS → DUPLICATE_SKIPPED smoke (Day 117) |
| `../proto/worker_execution.proto` | gRPC contract — regenerate with `make proto` |
| `internal/worker/executor.go` | `Executor` interface |
| `internal/worker/execution_result.go` | `ExecutionResult`, outcome status constants |
| `internal/worker/execution_result_test.go` | Unit tests for execution results |
| `internal/worker/result_event.go` | `WorkerResultEvent`, result topic contract |
| `internal/worker/result_event_test.go` | Unit tests for result events |
| `internal/worker/result_producer.go` | `ResultProducer`, `RecordingResultProducer` |
| `internal/worker/result_producer_test.go` | Unit tests for result producer |
| `internal/worker/kafka_result_producer.go` | `KafkaResultProducer` |
| `internal/worker/kafka_result_producer_test.go` | Unit tests for Kafka result producer |
| `internal/worker/kafka_consumer.go` | `KafkaConsumer`, `ProcessKafkaMessage` |
| `internal/worker/dlq.go` | `DeadLetterEvent`, `DeadLetterProducer` |
| `internal/worker/kafka_dlq_producer.go` | `KafkaDeadLetterProducer` |
| `cmd/consumer/main.go` | Kafka consumer entrypoint (poll loop + shutdown) |

There is **no real job execution** yet. Offset commits, retries, and Postgres updates come in later milestones.

## Dispatch Event Contract

Workers will eventually consume dispatch events from Kafka topic **`kernelq.jobs.dispatch`**.

- **`DispatchEvent`** in `internal/worker/dispatch_event.go` matches the JSON published by the Python control plane.
- **`ParseDispatchEvent`** parses and validates each message before execution.
- This protects workers from malformed or invalid Kafka messages (bad JSON, missing fields, wrong state/event type).

Run all worker tests (including dispatch-event parsing/validation):

```bash
go test ./...
```

## Consumer Message Processing

The worker plane now has **`ConsumerRunner`** in `internal/worker/consumer.go`. It is the **message-processing boundary**: raw **`Message.Value`** bytes go in, a validated **`DispatchEvent`** comes out, then a **`DispatchHandler`** runs.

- **`ProcessMessage`** calls **`ParseDispatchEvent`** (parse + validate) before any handler logic.
- Invalid JSON or bad field values return an error—workers do not execute malformed messages.

```bash
go test ./...
```

## Execution Handler

**`DispatchEventHandler`** (`internal/worker/handler.go`) converts validated **`DispatchEvent`** values into **`Task`** objects and calls an **`Executor`**.

- **`Executor`** is an interface for job execution (`Execute(task Task) (ExecutionResult, error)`).
- Current tests use a **fake executor** that records tasks and simulates outcomes.
- **Real execution**, **bounded concurrency**, and **Postgres status reporting** come later.

```bash
go test ./...
```

## Execution Results

**`ExecutionResult`** in `internal/worker/execution_result.go` classifies how a job attempt finished. Workers now return one of three outcomes:

- **`succeeded`** — the job completed successfully.
- **`retryable_failure`** — the job failed but may succeed on a later attempt (for example a transient timeout).
- **`terminal_failure`** — the job failed and should not be retried automatically (for example invalid payload or max retries exhausted).

**`DispatchEventHandler.Handle`** validates the executor’s result before returning it upstream. A plain Go **`error`** still means infrastructure failure (for example Postgres unreachable)—not a retry decision.

These structured results prepare **future retry logic** (publish to **`kernelq.jobs.retry`**, honor `retry_count` / `max_retries`) and **Postgres job-state updates** (`succeeded`, `failed`, `dead_lettered`) without guessing from error strings.

```bash
go test ./...
```

## Worker Result Events

Workers will publish **`WorkerResultEvent`** messages after execution. **`NewWorkerResultEvent`** maps an **`ExecutionResult`** onto JSON (`event_type: job.result`, `job_id`, `status`, `message`, `worker`).

- Result events go to **`kernelq.jobs.results`** (see `ResultTopic` in `internal/worker/result_event.go`).
- The **control plane will later consume** these events and **update Postgres job state** (`succeeded`, `failed`, `dead_lettered`, retry scheduling).
- **Today:** schema + **`Validate()`** / **`ToJSON()`** + unit tests; Kafka producer wired in **`cmd/consumer`** (see **Kafka Result Producer**).

```bash
go test ./...
```

## Result Producer Boundary

Workers now have a **`ResultProducer`** interface (`PublishResult(event WorkerResultEvent) error`) in `internal/worker/result_producer.go`.

- It publishes validated **`WorkerResultEvent`** records to **`kernelq.jobs.results`** when wired on **`DispatchEventHandler`**.
- **`RecordingResultProducer`** is an **in-memory** implementation for tests—it validates and appends events to **`Published`** without a broker.
- **`KafkaResultProducer`** publishes to the broker (see **Kafka Result Producer**).

```bash
go test ./...
```

## Kafka Result Producer

**`KafkaResultProducer`** in `internal/worker/kafka_result_producer.go` publishes **`WorkerResultEvent`** JSON to **`kernelq.jobs.results`**.

- **`PublishResult`** validates, encodes JSON, produces with **`JobID`** as the Kafka key, and flushes.
- **Tests** use a **fake `KafkaProducerClient`** (no real broker).
- **`cmd/consumer`** creates the real result producer at **`localhost:9092`** and passes it to **`DispatchEventHandler`** (see **Execution Result Publishing**).

```bash
go test ./...
go run ./cmd/consumer
```

## Execution Result Publishing

**`DispatchEventHandler`** can now publish **`WorkerResultEvent`** after **`Executor.Execute`** when a **`ResultProducer`** is configured.

- After a valid **`ExecutionResult`**, the handler calls **`NewWorkerResultEvent`** and **`PublishResult`**.
- **`ResultProducer`** is **optional**—tests use **`RecordingResultProducer`** or omit it entirely.
- **`cmd/consumer`** wires **`KafkaResultProducer`** with **`WorkerName: kernelq-go-worker`**.
- **Python control-plane result consumer** comes later (read **`kernelq.jobs.results`**, update Postgres).

```bash
go test ./...
go run ./cmd/consumer
```

## Worker Result Smoke Test

**`worker/scripts/smoke_worker_result.sh`** verifies the Kafka path from **`kernelq.jobs.dispatch`** to **`kernelq.jobs.results`**. Run it from the **repository root** (requires Docker, Go, and local Kafka on `localhost:9092`).

The script starts Kafka, builds and runs the worker, produces a valid **`DispatchEvent`**, consumes from the results topic, and checks for a matching **`job_id`**. It does **not** update Postgres—that requires the Python **result consumer** (not built yet).

```bash
./worker/scripts/smoke_worker_result.sh
```

## Worker Queue Saturation Smoke Test

**`worker/scripts/smoke_queue_saturation.sh`** verifies bounded-queue saturation stats on the consumer enqueue path. Run it from the **repository root** (Go only — **no Kafka**).

The underlying test uses **`worker_count=1`** and **`queue_capacity=1`**, blocks worker execution with a barrier so the queue fills, submits multiple jobs quickly, and expects **`work_queue_full_errors > 0`** (plus **`work_items_enqueued > 0`** and **`work_queue_capacity == 1`**).

```bash
./worker/scripts/smoke_queue_saturation.sh
```

## Kafka Consumer

The worker now includes **`KafkaConsumer`** in `internal/worker/kafka_consumer.go`. It adapts confluent-kafka-go records into our in-memory **`Message`** type and passes them to **`ConsumerRunner`**.

- **`ProcessKafkaMessage`** maps `*kafka.Message` key/value fields onto **`Message`**, then calls **`ConsumerRunner.ProcessMessage`**.
- **`ConsumerRunner`** handles parsing and validation (`ParseDispatchEvent`) before any handler runs.

```bash
go test ./...
```

## Kafka Poll Loop

**`KafkaConsumer.Run`** in `internal/worker/kafka_consumer.go` polls the broker until **`context.Context`** is canceled.

- **`cmd/consumer`** wires the full stack (Kafka → decode → **bounded worker pool** → `DispatchEventHandler` → logging executor) and runs **`Run(ctx, 1000)`**.
- The worker **continuously polls** `kernelq.jobs.dispatch` for new messages.
- **SIGINT / SIGTERM** cancel the context; **`Run`** closes the poller and exits cleanly.
- **Offset commits**, **retries**, and **real execution** are still future work—today a no-op executor prints received job ids.

```bash
go test ./...
go run ./cmd/consumer
```

## Worker Pool and Bounded Queue

**`WorkerPool`** (`internal/worker/worker_pool.go`) runs jobs on a fixed number of goroutines (**default 4**). The Kafka poll loop decodes messages and enqueues work; pool workers call **`DispatchEventHandler.Handle`**.

- **Bounded work queue** — buffered channel caps waiting jobs (**default capacity 100**). This is KernelQ’s **first backpressure boundary** on the worker side: memory stays bounded and saturation is visible.
- **Configure queue size** — set **`KERNELQ_WORKER_QUEUE_CAPACITY`** before starting **`cmd/consumer`** (unset or `<= 0` → default **100**). Startup logs **`worker_count=… queue_capacity=…`**.
- **Queue full** — non-blocking **`Enqueue`**; on **`worker queue full`**: log **`event=worker_queue_full`**, increment **`work_queue_full_errors`**, **50ms backoff**, **retry enqueue once**. If the retry succeeds, processing continues; if not, the job is dropped (no DLQ). Poll loop **keeps running**—still **not Kafka pause/resume**.
- **Saturation stats** — **`work_queue_capacity`**, **`work_queue_depth`**, **`work_items_enqueued`**, **`work_queue_full_errors`**, **`backpressure_pause_events`**, **`backpressure_resume_events`** (when policy wired).
- **Shutdown summary** — above work-queue stats plus **`messages_seen`**, **`messages_processed`**, **`message_errors`**, **`kafka_errors`**.
- **Future work** — **worker autoscaling** (today: tune **`KERNELQ_WORKER_COUNT`** / **`KERNELQ_WORKER_QUEUE_CAPACITY`** manually). See **Kafka Pause/Resume Backpressure** below.

```bash
go test ./...
KERNELQ_WORKER_QUEUE_CAPACITY=50 go run ./cmd/consumer
```

## Backpressure Configuration

**`cmd/consumer`** reads backpressure settings from the environment. **Disabled by default**—the worker runs with no **`BackpressurePolicy`** or **`PauseResumeController`** unless you opt in.

| Env var | Type | Default | Purpose |
|---------|------|---------|---------|
| **`KERNELQ_WORKER_BACKPRESSURE_ENABLED`** | bool (`true` / `false`) | **`false`** | Turn on watermark-based pause/resume wiring in **`KafkaConsumer`**. |
| **`KERNELQ_WORKER_BACKPRESSURE_HIGH_RATIO`** | float (`0`–`1`) | **`0.80`** | Queue depth ratio at which intake should **pause** (high watermark). |
| **`KERNELQ_WORKER_BACKPRESSURE_LOW_RATIO`** | float (`0`–`1`) | **`0.50`** | Queue depth ratio at which intake should **resume** (low watermark; must be **below** high). |

When **`KERNELQ_WORKER_BACKPRESSURE_ENABLED=true`**, **`cmd/consumer`** builds **`NewBackpressurePolicy(high, low)`** and **`NewInMemoryPauseResumeController()`** and passes both to **`KafkaConsumer`**. Invalid or inconsistent ratios fall back to **0.80 / 0.50** inside **`NewBackpressurePolicy`**. Startup logs include **`backpressure_enabled=…`**, **`backpressure_high_ratio=…`**, and **`backpressure_low_ratio=…`** (alongside **`worker_count`** and **`queue_capacity`**).

**Controller today:** **`InMemoryPauseResumeController`** is an **in-memory / test boundary**—it records pause/resume decisions and drives policy tests without touching the broker. **Real Kafka partition `Pause`/`Resume`** will come in a later milestone (Kafka adapter replacing the in-memory controller).

```bash
go test ./cmd/consumer -v
KERNELQ_WORKER_BACKPRESSURE_ENABLED=true \
  KERNELQ_WORKER_BACKPRESSURE_HIGH_RATIO=0.80 \
  KERNELQ_WORKER_BACKPRESSURE_LOW_RATIO=0.50 \
  go run ./cmd/consumer
```

## Backpressure Config Smoke Test

**`worker/scripts/smoke_backpressure_config.sh`** verifies **`cmd/consumer`** startup logs for backpressure env config. Run from the **repository root** (requires Docker, Go, and Kafka on **`localhost:9092`**).

The script builds the consumer, runs it briefly (background + **SIGINT**), and checks:

- Default: **`backpressure_enabled=false`**
- Enabled: **`backpressure_enabled=true`**, **`backpressure_high_ratio=0.8`**, **`backpressure_low_ratio=0.5`**

```bash
./worker/scripts/smoke_backpressure_config.sh
```

## Worker Throughput Benchmark

**Day 91+:** **`worker/scripts/benchmark_worker_throughput.sh`** complements scheduler throughput benchmarks (Day 75/77). Prefix-isolated runs; polls **`kernelq.jobs.results`** and exits when all matching **`job_id`** values are observed. **`TRIALS`** (default **`1`**) repeats runs; **`TRIALS>1`** reports **min/avg/max** worker throughput. Tune via **`COUNT`**, **`WORKERS`**, **`QUEUE_CAPACITY`**, **`TRIALS`**. **Local dev only — not production capacity.** Report: **[day91-worker-throughput.md](../docs/benchmarks/day91-worker-throughput.md)**.

```bash
COUNT=25 WORKERS=4 QUEUE_CAPACITY=100 ./worker/scripts/benchmark_worker_throughput.sh
TRIALS=3 COUNT=25 ./worker/scripts/benchmark_worker_throughput.sh
```

**Day 94+ end-to-end:** **`control_plane/scripts/benchmark_end_to_end_completion.sh`** — **`queued` → `succeeded`**; **`TRIALS`** for **min/avg/max** completion throughput when **`TRIALS>1`**. **Local dev only.** Report: **[day94-end-to-end-completion.md](../docs/benchmarks/day94-end-to-end-completion.md)**.

## Kafka Pause/Resume Backpressure

When enabled (see **Backpressure Configuration**), **`KafkaConsumer.Run`** wires **`BackpressurePolicy`** decisions to **`PauseResumeController`** via **`maybeApplyBackpressure`** (before poll, after enqueue, after worker success)—logs **`event=worker_backpressure_pause`** / **`event=worker_backpressure_resume`**; shutdown stats include **`backpressure_pause_events`** and **`backpressure_resume_events`**.

**Day 82** local queue-full backoff remains the fallback when backpressure is disabled or the queue is saturated before watermarks trigger.

## Invalid Message Handling

The worker **no longer exits** when it sees a malformed dispatch message (bad JSON, failed validation, handler error).

- **`Run`** increments **`MessageErrors`** and **keeps polling** so one poison record does not stop the whole process.
- When **`DeadLetterProducer`** is wired, failures also publish a **`DeadLetterEvent`** (see **DLQ Routing Boundary**).
- **`cmd/consumer`** prints **`message_errors`**, work-queue stats (**`work_queue_capacity`**, **`work_queue_depth`**, **`work_items_enqueued`**, **`work_queue_full_errors`**), and DLQ stats in the shutdown summary.
- **Kafka broker errors** (`kafka.Error`) still **stop the worker** for now.

## Dead Letter Queue Boundary

The worker defines a **`DeadLetterEvent`** shape in `internal/worker/dlq.go` for messages that cannot be processed on **`kernelq.jobs.dispatch`**.

- Fields include **`reason`**, original key/value, **`source_topic`**, and **`worker`** identity.
- **`DeadLetterProducer`** is the publish interface—see **DLQ Routing Boundary** for how **`KafkaConsumer.Run`** uses it.

## DLQ Routing Boundary

When processing fails, **`KafkaConsumer.Run`** can route invalid messages through **`DeadLetterProducer`**.

- On **`ProcessKafkaMessage`** error: increment **`MessageErrors`**, build **`DeadLetterEvent`**, call **`PublishDeadLetter`**, then **keep polling**.
- Stats track **`DeadLettersPublished`** and **`DeadLetterPublishErrors`**.
- **Tests** use a **fake producer** that captures events without a broker.
- **`cmd/consumer`** wires **`KafkaDeadLetterProducer`** (see **Kafka DLQ Producer**).

## Kafka DLQ Producer

**`KafkaDeadLetterProducer`** in `internal/worker/kafka_dlq_producer.go` publishes **`DeadLetterEvent`** JSON to **`kernelq.jobs.dlq`**.

- Invalid dispatch messages can be **preserved** on the DLQ topic—not only counted and skipped.
- **`PublishDeadLetter`** validates, encodes JSON, produces with **`OriginalKey`** as the Kafka key, and flushes.
- **Tests** use a **fake `KafkaProducerClient`** (no real broker).
- **`cmd/consumer`** creates the real DLQ producer at **`localhost:9092`** and passes it to **`KafkaConsumer`**.

```bash
go test ./...
go run ./cmd/consumer
```

## Prerequisites

- **Go 1.22+** installed (`go version`)
- For full end-to-end tests later: Postgres + Kafka (see repo root `docker-compose.yml`)

## Run tests

From the repository root:

```bash
cd worker
go test ./...
```

Verbose output for the task package only:

```bash
go test ./internal/worker/ -v
```

## Related docs

- Control plane (Python): `control_plane/README.md`
- Manual scheduler → Kafka smoke test: `docs/deploy.md`
- Architecture: `docs/architecture.md`
