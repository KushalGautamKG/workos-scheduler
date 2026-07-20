# Day 120 — Worker Execution Tracing

Functional validation of the first `worker.execute` span via stdout exporter. **No latency measurements.**

## Flow

1. Start `cmd/grpc-server` with `KERNELQ_OTEL_EXPORTER=stdout`.
2. One `Execute` via `cmd/grpc-execute`.
3. SIGINT server (flush batch processor).
4. Assert exporter output contains `worker.execute`, `job.id`, `job.attempt`, `execution.status`.

Harness: `./worker/scripts/smoke_worker_trace.sh`

## Explicit non-goals

- No distributed parent/child across Kafka yet
- No OTLP / collector
- No p50/p99 claims

## Related

- [worker-tracing.md](../design/worker-tracing.md)
- [day119-otel-foundation.md](day119-otel-foundation.md)
