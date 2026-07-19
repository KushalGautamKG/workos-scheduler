# Day 117 — gRPC Loopback Smoke

Functional validation of localhost `WorkerExecutionService` over a real gRPC listener. Not a latency benchmark.

## Scope

| Item | Detail |
|------|--------|
| Transport | Plain TCP gRPC on `127.0.0.1:50051` (configurable) |
| Path | Client → protobuf serialize → server → `DispatchEventHandler` → response |
| Duplicate | In-memory execution claim; second identical `Execute` → `DUPLICATE_SKIPPED` |
| Claims | **None** — no p50/p99 or throughput numbers |

## Workflow

Harness: `./worker/scripts/smoke_grpc_execute.sh`

1. Start `cmd/grpc-server` (memory idempotency).
2. `cmd/grpc-execute` with `job_id=test-job`, `attempt=1`, `payload=test` → `status=SUCCESS`.
3. Repeat identical request → `status=DUPLICATE_SKIPPED`, `duplicate_skipped=true`.
4. SIGINT graceful shutdown.

## What this validates

- Network listener registration and dial
- Request/response protobuf mapping
- Handler reuse (same execution path as Day 116 in-process tests)
- Duplicate status mapping across the wire
- Graceful server stop for deterministic smokes

## What this does not validate

- Kafka dispatch wiring
- Production RPC routing / service mesh
- TLS / auth
- Cross-host latency or capacity

## Related

- [grpc-worker-execution.md](../design/grpc-worker-execution.md)
- `worker/cmd/grpc-server`, `worker/internal/grpc/client.go`
