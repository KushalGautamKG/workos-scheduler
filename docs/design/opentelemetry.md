# OpenTelemetry Foundation

Day 119 introduces a **shared tracer provider** for KernelQ’s Go worker plane. No application spans yet—only configuration, resource metadata, global registration, and graceful shutdown.

**Interview sound bite:** *“Prometheus answers what; traces answer why—one provider for the process, interceptors later, OTLP when the collector exists.”*

---

## 1. Current vs Future

**Current:**

```
Metrics → Prometheus / Grafana
```

**Future (after Days 119–122):**

```
Trace
  │
Scheduler → Kafka Publish → Kafka Consume → Worker → gRPC Execute → Result
```

Day 119 only installs the shared provider that those hops will use.

---

## 2. Concepts

| Term | Role |
|------|------|
| **Trace** | End-to-end story of one logical request / job attempt |
| **Span** | One timed unit of work inside a trace |
| **Context propagation** | Carries parent span IDs across gRPC / Kafka so hops stitch together |
| **Exporter** | Where finished spans go (`stdout` today; **OTLP deferred**) |
| **Tracer provider** | Process-wide factory for tracers; configure once |

Instrumentation stays in interceptors/wrappers—not inside `DispatchEventHandler` business logic.

---

## 3. Configuration

| Env | Default | Notes |
|-----|---------|-------|
| `KERNELQ_OTEL_ENABLED` | `true` | `false` → no-op provider |
| `KERNELQ_OTEL_SERVICE_NAME` | `kernelq-worker` | Resource `service.name` |
| `KERNELQ_OTEL_EXPORTER` | `stdout` | `stdout` \| `none` |
| `KERNELQ_OTEL_SERVICE_VERSION` | `dev` | Optional |
| `KERNELQ_OTEL_ENVIRONMENT` | `local` | `deployment.environment` |

Package: `worker/internal/telemetry`. Wired from `cmd/grpc-server` (lifecycle only).

---

## 4. Status

| Item | Day |
|------|-----|
| Config + resource + provider | ✅ 119 |
| stdout / none exporters | ✅ 119 |
| Global `otel.SetTracerProvider` | ✅ 119 |
| Provider shutdown on SIGINT | ✅ 119 |
| `worker.execute` spans | ✅ 120 — [worker-tracing.md](worker-tracing.md) |
| gRPC interceptors / Kafka propagation | ❌ 121+ |
| OTLP exporter | ❌ Deferred |

---

## Related

- [day119-otel-foundation.md](../benchmarks/day119-otel-foundation.md)
- [grpc-lifecycle.md](grpc-lifecycle.md)
- `worker/internal/telemetry/`
