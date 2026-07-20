# Day 121 — gRPC Trace Propagation (Verification Note)

Functional verification only — **not** a throughput or latency benchmark.

## What was verified

| Check | Result |
|-------|--------|
| Loopback gRPC with otelgrpc client + server handlers | Pass |
| Parent context propagates (same TraceID client → server → `worker.execute`) | Pass |
| Error path: RPC spans end; execution error recorded when handler runs | Pass |
| Validation rejection: RPC spans end; no `worker.execute` | Pass |
| Exporter | In-memory (`tracetest`) in unit tests; stdout in smoke |

## Hierarchy observed

```
test.root / caller
  └── RPC client (SpanKindClient)
         └── RPC server (SpanKindServer, remote parent)
                └── worker.execute
```

Exact RPC span names follow OpenTelemetry semantic conventions (e.g. `…/Execute`); assertions prefer span kind + shared TraceID over brittle name strings.

Current `otelgrpc` (contrib v0.69) emits **`rpc.system.name=grpc`** and packs the full method into **`rpc.method`** (service name is not always a separate `rpc.service` attribute).

## Smoke

```bash
./worker/scripts/smoke_grpc_trace.sh
# PASS: grpc tracing smoke succeeded
# event=smoke_grpc_trace success=true
```

## Related

- [grpc-tracing.md](../design/grpc-tracing.md)
- [day120-worker-tracing.md](day120-worker-tracing.md)
