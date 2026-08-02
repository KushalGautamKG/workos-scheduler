# KernelQ

**Production-style distributed job orchestration** — Python control plane (API, scheduling, Postgres) + Go workers (Kafka consume/execute/publish) with Redis idempotency, OpenTelemetry, Prometheus/Grafana, structured logging, and Kubernetes/EKS preparation.

> KernelQ is a production-style portfolio implementation validated locally and through offline cloud configuration checks. It is **not** currently claimed to operate as a production service on AWS.

**Day 130 status:** implementation-complete for portfolio scope — [docs/checkpoints/day130.md](docs/checkpoints/day130.md)

## Architecture

```
Client → FastAPI → PostgreSQL → Weighted-Fair Scheduler → Kafka Dispatch
   → Go Worker Pool (backpressure, Redis claim, OTel) → Kafka Results
   → Python Result Consumer → PostgreSQL
```

Full diagram and technology roles: **[docs/architecture-final.md](docs/architecture-final.md)**

## Key capabilities

- Weighted-fair scheduling and atomic Postgres job claiming
- Kafka dispatch/result streams with concurrent Go workers
- Bounded queues, backpressure signals, retries, and dead-letter handling
- Redis idempotency at dispatch, execution, and result boundaries
- gRPC execution service + health probes
- Prometheus metrics, Grafana dashboards, OpenTelemetry traces, structured JSON logs
- Docker images, Kubernetes overlays, EKS deployment preparation
- Deterministic fault injection and recovery smokes

## Why it exists

KernelQ demonstrates an end-to-end **control plane / worker plane** design suitable for interview and portfolio depth: correctness under at-least-once delivery, operational observability, and honest limits (see [known-limitations.md](docs/known-limitations.md)).

## Quick start

```bash
docker compose up -d postgres zookeeper kafka redis
./infra/kafka/create-topics.sh
./control_plane/scripts/smoke_full_completion.sh
```

## Local demo

See **[docs/demo.md](docs/demo.md)** (10–15 minute and 5-minute interview flows).

## Benchmarks

Local baselines only (not production capacity):

| Report | Focus |
|--------|--------|
| [day75-baseline.md](docs/benchmarks/day75-baseline.md) | Enqueue / scheduler / latency plumbing |
| [day77-scheduler-1000.md](docs/benchmarks/day77-scheduler-1000.md) | Scheduler claim throughput (1k × 3) |
| [day91-worker-throughput.md](docs/benchmarks/day91-worker-throughput.md) | Worker Kafka path |
| [day94-end-to-end-completion.md](docs/benchmarks/day94-end-to-end-completion.md) | Full-path completion |
| [resume-metrics.md](docs/resume-metrics.md) | What is safe to quote on a resume |

## Reliability

Idempotency smokes, retry/DLQ smokes, and Day 129 resilience suite — [resilience-testing.md](docs/design/resilience-testing.md). Execution recovery after claim remains a **documented gap**.

## Observability

| Piece | Notes |
|-------|--------|
| Prometheus / Grafana | Local compose; Day 128 rules/dashboard |
| OpenTelemetry | Execute / gRPC / Kafka traces |
| Structured logs → Fluent Bit | CloudWatch-oriented config offline-validated |

## Deployment

Kustomize: `base` → `local` → `production` → `eks` → `eks-observability`. EKS/ECR scripts support dry-run; offline smoke: `./worker/scripts/smoke_eks_config.sh`.

## Testing

```bash
python3 -m pytest control_plane/tests
cd worker && go test ./...
./worker/scripts/smoke_day130.sh
```

## Known limitations

**[docs/known-limitations.md](docs/known-limitations.md)** — crash-after-claim gap, offline cloud config, no production SLO claims.

## Project status

| Doc | Purpose |
|-----|---------|
| [architecture-final.md](docs/architecture-final.md) | Final architecture |
| [capability-evidence-matrix.md](docs/capability-evidence-matrix.md) | Capability ↔ evidence |
| [production-readiness.md](docs/production-readiness.md) | Readiness checklist |
| [known-limitations.md](docs/known-limitations.md) | Honest gaps |
| [demo.md](docs/demo.md) | Demo script |
| [interview-guide.md](docs/interview-guide.md) | Interview Q&A |
| [resume-metrics.md](docs/resume-metrics.md) | Resume number evidence |
| [checkpoints/day130.md](docs/checkpoints/day130.md) | Completion checkpoint |
| [mvp.md](docs/mvp.md) | MVP talking points |
| [runbooks.md](docs/runbooks.md) | Operations index |

## Observability (local URLs)

| Service | URL |
|---------|-----|
| Prometheus | http://127.0.0.1:9090 |
| Grafana | http://127.0.0.1:3000 (`admin` / `admin`) |
