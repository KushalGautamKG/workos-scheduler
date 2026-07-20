# gRPC Trace Propagation

Day 121 connects OpenTelemetry across the gRPC boundary so a client request and `worker.execute` share one distributed trace.

**Interview sound bite:** *“otelgrpc owns the transport spans—W3C context rides in metadata, and worker.execute nests under the server span.”*

---

## Before Day 121

```
gRPC request          worker.execute
(local / no parent)   (local root)
```

Client and worker spans could be unrelated roots even for one RPC.

## After Day 121

```
caller
  └── gRPC client span (SpanKindClient)
         └── gRPC server span (SpanKindServer)
                └── worker.execute
```

---

## Mechanism

| Piece | Role |
|-------|------|
| **W3C Trace Context** | `traceparent` / `tracestate` in gRPC metadata |
| **otelgrpc** | Official StatsHandlers inject (client) and extract (server) |
| **Propagator** | Global `TraceContext` + `Baggage` TextMapPropagator |
| **Caller context** | Client timeout derived from caller `ctx` so the parent span stays linked |

Helpers: `telemetry.GRPCServerOptions()` / `GRPCDialOptions()` — no manual `traceparent` parsing.

---

## Semantics

- **Span kinds:** client vs server RPC spans; execution remains an internal `worker.execute` span.
- **Request context:** server handler receives the extracted context; `DispatchEventHandler` starts `worker.execute` as a child.
- **Errors / deadlines:** RPC status on transport spans; execution errors recorded on `worker.execute` when the handler runs. Validation failures may end RPC spans without `worker.execute`.
- **Payloads:** not recorded as span attributes.
- **Trace IDs:** observability identifiers only — **not** job idempotency keys.

---

## Deferred

| Item | When |
|------|------|
| Kafka header propagation | Day 122 |
| OTLP export | Later (stdout remains verification path) |

---

## Related

- [worker-tracing.md](worker-tracing.md) — `worker.execute`
- [opentelemetry.md](opentelemetry.md) — provider foundation
- [day121-grpc-tracing.md](../benchmarks/day121-grpc-tracing.md)
- `./worker/scripts/smoke_grpc_trace.sh`
