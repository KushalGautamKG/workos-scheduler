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

There is **no Kafka consumer**, **no main entrypoint**, and **no job execution loop** yet. Those come in later milestones after this skeleton is in place.

## Dispatch Event Contract

Workers will eventually consume dispatch events from Kafka topic **`kernelq.jobs.dispatch`**.

- **`DispatchEvent`** in `internal/worker/dispatch_event.go` matches the JSON published by the Python control plane.
- **`ParseDispatchEvent`** parses and validates each message before execution.
- This protects workers from malformed or invalid Kafka messages (bad JSON, missing fields, wrong state/event type).

Run all worker tests (including dispatch-event parsing/validation):

```bash
go test ./...
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
