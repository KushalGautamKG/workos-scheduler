# KernelQ Worker Plane (Go)

This folder is the **Go worker plane** for KernelQ—the half of the system that **executes** jobs after the **Python control plane** decides they are ready to run.

## What workers will do (later)

Today the control plane **schedules** work: jobs live in Postgres, scheduler ticks claim them, and **dispatch events** are published to Kafka topic **`kernelq.jobs.dispatch`**.

**Go workers will eventually:**

1. **Consume dispatch events** from Kafka (same JSON shape as Python `DispatchEvent`: `job_id`, `tenant_id`, `priority`, `payload`, …).
2. **Validate and run** each job using bounded concurrency—only *N* jobs at a time per worker process so memory and CPU stay predictable under load.
3. **Report state back** to Postgres (`running`, `succeeded`, `failed`, etc.).

Kafka carries the handoff; Postgres stays the system of record. See `docs/architecture.md` and `docs/decisions/ADR-0001-foundations-and-language-split.md`.

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
| `internal/worker/executor.go` | `Executor` interface |
| `internal/worker/kafka_consumer.go` | `KafkaConsumer`, `ProcessKafkaMessage` |
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

- **`Executor`** is an interface for future job execution (`Execute(task Task) error`).
- Current tests use a **fake executor** that records tasks and simulates failures.
- **Real execution**, **bounded concurrency**, and **Postgres status reporting** come later.

```bash
go test ./...
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

- **`cmd/consumer`** wires the full stack (Kafka → `ConsumerRunner` → `DispatchEventHandler` → logging executor) and runs **`Run(ctx, 1000)`**.
- The worker **continuously polls** `kernelq.jobs.dispatch` for new messages.
- **SIGINT / SIGTERM** cancel the context; **`Run`** closes the poller and exits cleanly.
- **Offset commits**, **retries**, and **real execution** are still future work—today a no-op executor prints received job ids.

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
