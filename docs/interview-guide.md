# Interview Guide (Day 130)

Answers match the **actual** KernelQ implementation.

## Why Kafka instead of direct RPC for dispatch?

Async handoff, consumer-group scaling, durable buffering, and natural retry/replay. gRPC exists for **internal** execute/health, not as the primary scheduler→worker dispatch bus.

## Why PostgreSQL and Redis?

Postgres is the **system of record** for job state and atomic claims. Redis is a **fast idempotency boundary** (dispatch / execution / result keys) for near-term duplicate suppression under Kafka at-least-once delivery—not a replacement for durable state.

## Why write workers in Go?

High-concurrency consume/execute path: goroutine pool, bounded queues, efficient Kafka client usage. Control plane stays Python for scheduling/API velocity.

## How does weighted fairness work?

Control-plane scheduling policy shares dispatch opportunity across tenants (weighted) with priority within a tenant; Postgres listing/claim enforces durable ordering constraints. Exact policy modules live in the scheduler/composed-scheduler path—workers do not decide global fairness.

## How does backpressure work?

A bounded work queue rejects or delays intake when full; watermark **BackpressurePolicy** drives an in-memory **PauseResumeController**. This is local visibility/throttling—not a proven production Kafka partition Pause/Resume loop.

## How do retries avoid infinite loops?

`retry_count` / `max_retries`, `retry_scheduled` + `retry_after`, scanner requeue, then **dead_lettered** on exhaustion. Manual requeue is an operator path for DLQ recovery.

## How are duplicates suppressed?

Three Redis/memory claim layers: dispatch key before publish, execution key before `Execute`, result key before applying worker results. Duplicates skip side effects; they are expected under at-least-once Kafka.

## What happens during Kafka replay?

Same `job_id`+`attempt` may be delivered twice. Execution claim makes the second `duplicate_skipped` (`executor_calls=1` in replay smoke). Multiple deliveries are OK; multiple completions are not.

## What happens when Redis is unavailable?

Execution path **fails closed** on claim errors (no unsafe execute). Restore Redis, then retry/redeliver. Do not silently bypass idempotency.

## What happens when a worker crashes after claiming a job?

**Known gap:** Redis claim may remain while no result is published; redispatch of the same attempt can skip until TTL/reconciliation. Documented in execution-recovery; lease/watchdog deferred.

## Why add gRPC if Kafka already exists?

Internal, synchronous execute + official health for probes/lifecycle. Kafka remains the async dispatch backbone.

## How does trace context cross Kafka?

W3C TraceContext injected into Kafka headers on publish and extracted on process; spans nest `kafka.publish` / `kafka.process` / `worker.execute` (and gRPC via otelgrpc).

## How would you scale workers?

Increase consumer instances/replicas in the same group; tune `KERNELQ_WORKER_COUNT` and queue capacity; watch lag, queue depth, and backpressure signals. Scale control plane separately (API/scheduler).

## What would you change before production?

Execution leases/watchdog; wire real broker pause/resume if needed; managed Prometheus/Grafana/Alertmanager; live EKS+CloudWatch with IAM; soak tests; measure real SLOs—not proposed targets.

## What metrics and alerts matter most?

Success/failure/retry rates, execution & queue latency p95, queue depth growth, worker availability, publish success, Redis/Kafka dependency errors—with runbooks linked from alert annotations.
