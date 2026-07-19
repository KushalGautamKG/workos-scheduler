# gRPC Worker Execution Service

Internal gRPC contract for worker execution. Kafka remains the async dispatch path; the RPC layer is additive.

**Interview sound bite:** *“Kafka moves work asynchronously; gRPC is the typed internal execution boundary we’ll instrument with OpenTelemetry later.”*

---

## 1. Current Architecture

```
Scheduler
    │
Kafka Dispatch          ← still the production dispatch layer
    │
Go Worker → DispatchEventHandler → Executor

                +

cmd/grpc-server (localhost)
    │
gRPC WorkerExecutionService
    │
same DispatchEventHandler
```

Day 117 adds a **network listener** and **client** validated on loopback. No production RPC routing yet.

---

## 2. Why gRPC

| Concern | gRPC fit |
|---------|----------|
| **Cross-language contract** | `.proto` shared by Go (now) and Python (later clients) |
| **Typed RPCs** | Codegen beats hand-rolled JSON DTOs for internal services |
| **Binary serialization** | Compact protobuf payloads for hot internal paths |
| **Interceptors** | Natural hook for OpenTelemetry tracing (Days 119–122) |
| **Future streaming** | Unary `Execute` today; streaming heartbeats/batches possible later |

---

## 3. Contract

File: [`proto/worker_execution.proto`](../../proto/worker_execution.proto)

- **Package:** `kernelq.worker.v1`
- **Service:** `WorkerExecutionService`
- **RPC:** `Execute(ExecuteRequest) → ExecuteResponse`
- **Statuses:** `SUCCESS`, `FAILED`, `DUPLICATE_SKIPPED`

```bash
make proto
```

---

## 4. Implementation Status

| Item | Status |
|------|--------|
| Proto + generated Go | ✅ Day 116 |
| In-process server + unit tests | ✅ Day 116 |
| Network listener (`cmd/grpc-server`) | ✅ Day 117 |
| Client + loopback tests / smoke | ✅ Day 117 |
| Health + readiness lifecycle | ✅ Day 118 — [grpc-lifecycle.md](grpc-lifecycle.md) |
| Centralized `KERNELQ_GRPC_*` config | ✅ Day 118 |
| Replace Kafka dispatch | ❌ Not a goal |
| TLS / auth / production routing | ❌ Deferred |
| OpenTelemetry interceptors | ❌ Day 119+ |
| Kubernetes probes wired | ❌ Documented; deploy later |

**Listener:** `KERNELQ_GRPC_ADDR` (default `127.0.0.1:50051`). Graceful shutdown on SIGINT/SIGTERM. Default idempotency backend for the gRPC server binary: **memory**.

**Client:** `worker/internal/grpc.Client` — dial endpoint, context timeout, no retries yet.

---

## 5. Non-Goals

- No change to Kafka consumer → handler wiring
- No Python gRPC client yet
- No authentication / mTLS
- No service-mesh or cross-host production routing

---

## Related

- [day117-grpc-loopback.md](../benchmarks/day117-grpc-loopback.md) — functional loopback note
- [worker-execution-idempotency.md](worker-execution-idempotency.md)
- [ADR-0001](../decisions/ADR-0001-foundations-and-language-split.md)
- `worker/cmd/grpc-server`, `worker/internal/grpc/`
