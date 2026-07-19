# gRPC Lifecycle and Health

Day 118 adds **readiness lifecycle** and **centralized gRPC config** so the localhost listener is operationally ready for Kubernetes probes later. Kafka remains the async dispatch path.

**Interview sound bite:** *“Serve first, mark SERVING second; on signal mark NOT_SERVING, then GracefulStop—liveness isn’t readiness.”*

---

## 1. Lifecycle States

| State | Meaning |
|-------|---------|
| **Starting** | Process up; health **NOT_SERVING**; listener not yet ready for traffic |
| **SERVING** | Listener up; execution + health registered; ready for new RPCs |
| **Shutting down** | Signal received; health **NOT_SERVING**; drain in-flight RPCs |
| **Stopped** | Server exited cleanly |

```
start → NOT_SERVING → Serve → SERVING
                              │
                         SIGINT/SIGTERM
                              │
                              ▼
                    NOT_SERVING → GracefulStop → stopped
```

---

## 2. Startup

1. Load `KERNELQ_GRPC_*` via `internal/config`
2. Create `grpc.health.v1` wrapper (**NOT_SERVING**)
3. Listen + register `WorkerExecutionService` + health
4. `Serve` in background
5. **MarkReady → SERVING**

Readiness fails until step 5 so probes do not send traffic at a half-initialized process.

---

## 3. Graceful Shutdown

On SIGINT/SIGTERM:

1. Log shutdown
2. **MarkNotReady → NOT_SERVING** (new readiness checks fail immediately)
3. `GracefulStop` with `KERNELQ_GRPC_SHUTDOWN_TIMEOUT`
4. Force `Stop` if the deadline expires
5. Log `event=grpc_server_stopped`

In-flight `Execute` calls get a chance to finish; clients see a clear serving-set drop before the socket closes.

---

## 4. Configuration

| Env | Default | Purpose |
|-----|---------|---------|
| `KERNELQ_GRPC_ADDR` | `127.0.0.1:50051` | Listen / dial address |
| `KERNELQ_GRPC_SHUTDOWN_TIMEOUT` | `10s` | Graceful stop deadline |
| `KERNELQ_GRPC_REQUEST_TIMEOUT` | `5s` | Client / health-check RPC timeout |

Parsing lives in `worker/internal/config` — not inside the server binary’s ad-hoc env reads.

---

## 5. Future

| Item | Notes |
|------|--------|
| **Kubernetes probes** | `grpc.health.v1` Check → readiness; process aliveness separate |
| **OpenTelemetry interceptors** | Span around `Execute` once lifecycle is stable (Days 119–122) |
| **Deep dependency checks** | Keep health cheap; Kafka/Redis status via metrics, not every probe |

---

## Related

- [grpc-worker-execution.md](grpc-worker-execution.md)
- [day118-grpc-health.md](../benchmarks/day118-grpc-health.md)
- `worker/internal/grpc/health.go`, `worker/cmd/grpc-server`
- `./worker/scripts/smoke_grpc_health.sh`
