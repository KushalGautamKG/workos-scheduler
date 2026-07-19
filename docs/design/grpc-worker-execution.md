# gRPC Worker Execution Service

Day 116 introduces an **internal gRPC contract** for worker execution. Kafka remains the async dispatch path; nothing in the live pipeline is replaced yet.

**Interview sound bite:** *“Kafka moves work asynchronously; gRPC is the typed internal execution boundary we’ll instrument with OpenTelemetry later.”*

---

## 1. Current vs Future

**Current (unchanged):**

```
Scheduler
    │
Kafka Dispatch
    │
Go Worker → DispatchEventHandler → Executor
```

**Day 116 (additive):**

```
Kafka
  │
Worker (consumer still owns intake)
  │
gRPC WorkerExecutionService   ← contract + in-process skeleton
  │
Execution Handler
```

**Later:**

```
Kafka → Worker → gRPC (network) → Execution Service → OpenTelemetry
```

---

## 2. Why gRPC

| Concern | gRPC fit |
|---------|----------|
| **Cross-language contract** | `.proto` shared by Go (now) and Python (later clients) |
| **Typed RPCs** | Codegen beats hand-rolled JSON DTOs for internal services |
| **Binary serialization** | Compact protobuf payloads for hot internal paths |
| **Interceptors** | Natural hook for OpenTelemetry tracing (Days 119–122) |
| **Future streaming** | Unary `Execute` today; streaming heartbeats/batches possible later |

REST stays appropriate for the **public** control-plane API. gRPC is for **internal** service decomposition.

---

## 3. Contract

File: [`proto/worker_execution.proto`](../../proto/worker_execution.proto)

- **Package:** `kernelq.worker.v1`
- **Service:** `WorkerExecutionService`
- **RPC:** `Execute(ExecuteRequest) → ExecuteResponse`
- **Statuses:** `SUCCESS`, `FAILED`, `DUPLICATE_SKIPPED`

Intentionally small: `job_id`, `attempt`, `payload` in; status / duplicate flag / error out.

Generate Go stubs:

```bash
make proto
```

Output: `worker/internal/grpc/pb/`.

---

## 4. Local Skeleton

`worker/internal/grpc.Server` implements the generated server interface and delegates to an `ExecutionHandler` (`*worker.DispatchEventHandler` satisfies it).

| Day 116 | Status |
|---------|--------|
| Proto + generated Go | ✅ |
| In-process `Execute` + validation | ✅ |
| Unit tests (no network) | ✅ |
| Network listener / client | ❌ Day 117–118 |
| Replace Kafka dispatch | ❌ Never Day 116 goal |
| Auth / mTLS | ❌ Deferred |
| OpenTelemetry interceptors | ❌ Day 119+ |

When `Handler` is nil, `Execute` returns gRPC **`Unimplemented`**.

---

## 5. Non-Goals

- No change to Kafka consumer wiring
- No Python gRPC client yet
- No authentication
- No production listener bind

---

## Related

- [execution-recovery.md](execution-recovery.md) — crash-after-claim (orthogonal)
- [worker-execution-idempotency.md](worker-execution-idempotency.md) — Redis claim before execute
- [ADR-0001](../decisions/ADR-0001-foundations-and-language-split.md) — language split / protocol need
- `worker/internal/grpc/` — server skeleton + tests
