# Day 118 — gRPC Health Lifecycle

Functional verification of readiness + graceful shutdown. **No performance measurements.**

## What was checked

| Step | Expected |
|------|----------|
| Start `cmd/grpc-server` | Logs `event=grpc_server_ready status=SERVING` |
| `grpc.health.v1` Check | `status=SERVING` |
| SIGINT | Health → `NOT_SERVING`, then `event=grpc_server_stopped` |
| Process exit | Clean exit within shutdown timeout |

Harness: `./worker/scripts/smoke_grpc_health.sh`

## Health check flow

```
client Check(service="")
        │
        ▼
grpc.health.v1 on KERNELQ_GRPC_ADDR
        │
        ▼
overall status SERVING | NOT_SERVING
```

No Kafka/Redis/Postgres calls on the probe path.

## Explicit non-goals

- No latency / QPS numbers
- No Kubernetes probe wiring yet (documented only)
- No TLS or auth
- Kafka dispatch path unchanged

## Related

- [grpc-lifecycle.md](../design/grpc-lifecycle.md)
- [day117-grpc-loopback.md](day117-grpc-loopback.md)
