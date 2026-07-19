# KernelQ

Distributed job orchestration prototype: **Python control plane** (API, scheduling, Postgres state) + **Go workers** (Kafka consume/execute/publish) + **Kafka** between the planes. **Day 117:** gRPC `WorkerExecutionService` **network listener** + loopback smoke (Kafka still dispatches; no production RPC routing) — **[docs/design/grpc-worker-execution.md](docs/design/grpc-worker-execution.md)**.

## MVP Status

KernelQ has reached an **MVP checkpoint**. See **[docs/mvp.md](docs/mvp.md)** for capabilities, demo commands, limitations, and resume talking points.

**Day 90 checkpoint:** **[docs/checkpoints/day90.md](docs/checkpoints/day90.md)** summarizes current platform status, benchmarks, limitations, and the post-MVP roadmap (production-readiness phase).

**Happy path (smoke-tested):**

```
queued job → scheduler → Kafka → Go worker → result event → Postgres SUCCEEDED
```

**Retry and dead-letter flows** are also smoke-tested: `retryable_failure` → `RETRY_SCHEDULED` → requeue → dispatch; exhaustion → `DEAD_LETTERED`; operator inspection and manual requeue.

**Observability:** `docker compose up -d prometheus grafana` (API on `:8000` required for scrapes). **Prometheus** — [http://127.0.0.1:9090](http://127.0.0.1:9090). **Grafana** — [http://127.0.0.1:3000](http://127.0.0.1:3000) (`admin` / `admin`). Dashboard **KernelQ MVP** charts **`kernelq_jobs_by_state`**. Local benchmark baseline (load generation, scheduler throughput, queue latency): **[docs/benchmarks/day75-baseline.md](docs/benchmarks/day75-baseline.md)** — dev numbers only, not production claims. See [docs/deploy.md](docs/deploy.md).

## Quick start

```bash
docker compose up -d postgres zookeeper kafka redis
./infra/kafka/create-topics.sh
./control_plane/scripts/smoke_full_completion.sh
```

## Observability

| Service | URL | Notes |
|---------|-----|-------|
| Prometheus | [http://127.0.0.1:9090](http://127.0.0.1:9090) | Scrapes `GET /metrics/prometheus` |
| Grafana | [http://127.0.0.1:3000](http://127.0.0.1:3000) | Login `admin` / `admin` |

Provisioned dashboard **KernelQ MVP** — **`kernelq_jobs_by_state`** and **Result Consumer Messages** (`kernelq_result_consumer_processed_messages`, `kernelq_result_consumer_duplicate_messages`). Config: [infra/prometheus/prometheus.yml](infra/prometheus/prometheus.yml). Local dev only.

## Docs

| Doc | Purpose |
|-----|---------|
| [docs/checkpoints/day90.md](docs/checkpoints/day90.md) | Day 90 checkpoint — platform status, benchmarks, limitations, roadmap |
| [docs/checkpoints/day115.md](docs/checkpoints/day115.md) | Day 115 — dedupe complete; execution recovery deferred |
| [docs/mvp.md](docs/mvp.md) | MVP checkpoint — demo, tests, talking points |
| [docs/architecture.md](docs/architecture.md) | System design |
| [docs/design/redis-idempotency-deduplication.md](docs/design/redis-idempotency-deduplication.md) | Redis idempotency/dedupe; dispatch + result integrated |
| [docs/design/worker-execution-idempotency.md](docs/design/worker-execution-idempotency.md) | Worker execution dedupe + Kafka replay smoke (Day 114) |
| [docs/design/execution-recovery.md](docs/design/execution-recovery.md) | Claim-before-completion gap; lease + watchdog (future) |
| [docs/design/grpc-worker-execution.md](docs/design/grpc-worker-execution.md) | Internal gRPC WorkerExecutionService (Days 116–117) |
| [docs/benchmarks/day117-grpc-loopback.md](docs/benchmarks/day117-grpc-loopback.md) | Localhost gRPC loopback functional note (Day 117) |
| [docs/benchmarks/day114-kafka-execution-replay.md](docs/benchmarks/day114-kafka-execution-replay.md) | Duplicate Kafka dispatch replay methodology (Day 114) |
| [docs/deploy.md](docs/deploy.md) | Local setup and smoke tests |
| [docs/runbooks.md](docs/runbooks.md) | Operational runbooks |
| [docs/benchmarks/day75-baseline.md](docs/benchmarks/day75-baseline.md) | Local benchmark baseline (not production claims) |
| [control_plane/README.md](control_plane/README.md) | Control plane details |
| [worker/README.md](worker/README.md) | Go worker details |

## Tests

```bash
python3 -m pytest control_plane/tests
cd worker && go test ./...
```
