# Day 119 — OpenTelemetry Foundation

Functional verification of tracer provider initialization. **No latency measurements. No spans asserted.**

## What was checked

| Check | Result |
|-------|--------|
| Enabled + `stdout` provider builds | Unit test |
| Disabled / `none` provider builds | Unit test |
| Invalid exporter rejected | Unit test |
| `Shutdown` succeeds | Unit test |
| `cmd/grpc-server` starts with provider | Manual / smoke lifecycle |
| Graceful stop shuts down provider | `event=otel_tracer_provider_stopped` |

## Explicit non-goals

- No span creation or interceptor wiring
- No OTLP / collector / Jaeger
- No cross-service propagation yet
- No performance claims

## Related

- [opentelemetry.md](../design/opentelemetry.md)
- `worker/internal/telemetry/`
