# Worker Execution Tracing

Day 120 adds the first meaningful OpenTelemetry span around job execution: **`worker.execute`**.

**Interview sound bite:** *“One span per execution attempt—job id and attempt as attributes, status and errors on the span, payloads stay out.”*

---

## 1. Current Trace

```
worker.execute
  attributes: job.id, job.attempt, execution.status[, duplicate_skipped]
```

Parent for future Kafka consume / gRPC / result-publish child spans (Days 121–122).

---

## 2. Span Lifecycle

```
StartExecutionSpan(ctx, jobID, attempt)
        │
        ▼
claim / Execute / publish   ← business logic (unchanged)
        │
        ▼
set execution.status (+ duplicate_skipped | RecordError)
        │
        ▼
defer span.End()
```

Helpers live in `worker/internal/telemetry` (`spans.go`, `attributes.go`).  
Instrumentation sits in `DispatchEventHandler.Handle`—not inside every helper function.

---

## 3. Attributes

| Key | Meaning |
|-----|---------|
| `job.id` | Job identity |
| `job.attempt` | Retry generation |
| `execution.status` | `success` \| `duplicate` \| `failed` |
| `duplicate_skipped` | `true` on duplicate claim |

**Not attached:** payload bodies (size/PII/cardinality).

---

## 4. Errors

Infrastructure and failed outcomes call `span.RecordError` and set span status `Error`. Duplicates are **not** errors (`Ok` + `execution.status=duplicate`).

---

## 5. Future

| Item | Notes |
|------|--------|
| Child spans | claim, publish result |
| Kafka propagation | inject/extract trace context on dispatch/results |
| gRPC propagation | ✅ Day 121 — [grpc-tracing.md](grpc-tracing.md) |
| OTLP | replace stdout when collector exists |

---

## Related

- [opentelemetry.md](opentelemetry.md) — provider foundation
- [day120-worker-tracing.md](../benchmarks/day120-worker-tracing.md)
- `./worker/scripts/smoke_worker_trace.sh`
