# Kafka Trace Propagation

Day 122 propagates OpenTelemetry W3C trace context through Kafka headers so the asynchronous dispatch path shares one distributed trace.

**Interview sound bite:** *“Kafka has no RPC metadata—inject `traceparent` into headers on publish, extract before `kafka.process`, and nest `worker.execute` underneath.”*

---

## Before Day 122

```
producer (local root)     worker.execute (unrelated root)
```

## After Day 122

```
caller / smoke root
  └── kafka.publish (dispatch)
         │  W3C headers
         ▼
      kafka.process
         └── worker.execute
                └── kafka.publish (result)
```

---

## Producer

```
start kafka.publish
      ↓
inject W3C into Kafka headers
      ↓
produce + wait for ack
```

Helpers: `telemetry.StartKafkaPublishSpan`, `InjectKafkaContext`.  
Go dispatch tooling: `worker.KafkaDispatchProducer` / `cmd/dispatch-publish`.  
Python `KafkaJobProducer` accepts optional `headers=` without changing the JSON event schema.

## Consumer

```
receive message
      ↓
ExtractKafkaContext(headers)
      ↓
start kafka.process
      ↓
DispatchEventHandler.Handle(ctx) → worker.execute
```

Missing or malformed `traceparent` → usable new trace; **never** blocks processing.

## Result path

`worker.execute` context → result `kafka.publish` → inject headers on `kernelq.jobs.results` for a future result consumer. Duplicates that skip publish do not invent a result span.

---

## Rules

| Topic | Rule |
|-------|------|
| Headers vs payload | Trace context in headers only — event schemas stay business-only |
| Retries / duplicates | Preserve original trace context; **TraceID ≠ idempotency key** |
| Payloads | Never span attributes |
| OTLP | Deferred (stdout verification) |
| Cross-language | W3C Trace Context |

---

## Related

- [grpc-tracing.md](grpc-tracing.md) — Day 121 synchronous path
- [worker-tracing.md](worker-tracing.md) — `worker.execute`
- [day122-kafka-tracing.md](../benchmarks/day122-kafka-tracing.md)
- `./worker/scripts/smoke_kafka_trace.sh`
