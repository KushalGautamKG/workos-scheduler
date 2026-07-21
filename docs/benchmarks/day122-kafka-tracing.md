# Day 122 — Kafka Trace Propagation (Verification Note)

Functional verification only — **not** a throughput or latency benchmark.

## What was verified

| Check | Result |
|-------|--------|
| W3C inject/extract via Kafka headers | Pass |
| `kafka.publish` → `kafka.process` → `worker.execute` same TraceID | Pass |
| Result `kafka.publish` continues the trace when published | Pass |
| Missing headers still process | Pass |
| Malformed `traceparent` ignored safely | Pass |
| Exporter | In-memory unit tests; stdout smoke |

## Hierarchy (manual parent-child model)

```
test.root / smoke root
  └── kafka.publish (SpanKindProducer)
         └── kafka.process (SpanKindConsumer, remote parent)
                └── worker.execute
                       └── kafka.publish result
```

## Smoke

```bash
./worker/scripts/smoke_kafka_trace.sh
# PASS: kafka trace smoke succeeded
# event=smoke_kafka_trace success=true
```

## Related

- [kafka-tracing.md](../design/kafka-tracing.md)
- [day121-grpc-tracing.md](day121-grpc-tracing.md)
