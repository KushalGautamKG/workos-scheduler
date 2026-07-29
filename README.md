# KernelQ

Distributed job orchestration prototype: **Python control plane** + **Go workers** + **Kafka**. **Day 129:** deterministic fault injection, dependency-outage / duplicate-delivery / crash-recovery smokes — **[docs/design/resilience-testing.md](docs/design/resilience-testing.md)**. Local validation only; not production chaos testing.

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
| [docs/design/grpc-lifecycle.md](docs/design/grpc-lifecycle.md) | gRPC health + readiness lifecycle (Day 118) |
| [docs/design/opentelemetry.md](docs/design/opentelemetry.md) | Shared OTel tracer provider foundation (Day 119) |
| [docs/design/worker-tracing.md](docs/design/worker-tracing.md) | worker.execute spans (Day 120) |
| [docs/design/grpc-tracing.md](docs/design/grpc-tracing.md) | gRPC client/server trace propagation (Day 121) |
| [docs/design/kafka-tracing.md](docs/design/kafka-tracing.md) | Kafka header trace propagation (Day 122) |
| [docs/design/containerization.md](docs/design/containerization.md) | Docker + Kubernetes foundation (Day 123) |
| [docs/design/local-kubernetes.md](docs/design/local-kubernetes.md) | Local Kubernetes rollout validation (Day 124) |
| [docs/design/kubernetes-production-policies.md](docs/design/kubernetes-production-policies.md) | Production K8s overlays + policies (Day 125) |
| [docs/design/eks-deployment.md](docs/design/eks-deployment.md) | EKS/ECR deployment preparation (Day 126) |
| [docs/design/structured-logging.md](docs/design/structured-logging.md) | Structured JSON logging + Fluent Bit (Day 127) |
| [docs/design/monitoring.md](docs/design/monitoring.md) | SLIs/SLOs, recording/alert rules, dashboards (Day 128) |
| [docs/design/resilience-testing.md](docs/design/resilience-testing.md) | Fault injection + resilience smokes (Day 129) |
| [docs/benchmarks/day117-grpc-loopback.md](docs/benchmarks/day117-grpc-loopback.md) | Localhost gRPC loopback functional note (Day 117) |
| [docs/benchmarks/day118-grpc-health.md](docs/benchmarks/day118-grpc-health.md) | gRPC health lifecycle functional note (Day 118) |
| [docs/benchmarks/day119-otel-foundation.md](docs/benchmarks/day119-otel-foundation.md) | OTel provider init functional note (Day 119) |
| [docs/benchmarks/day120-worker-tracing.md](docs/benchmarks/day120-worker-tracing.md) | worker.execute stdout smoke note (Day 120) |
| [docs/benchmarks/day121-grpc-tracing.md](docs/benchmarks/day121-grpc-tracing.md) | gRPC shared-trace verification note (Day 121) |
| [docs/benchmarks/day122-kafka-tracing.md](docs/benchmarks/day122-kafka-tracing.md) | Kafka shared-trace verification note (Day 122) |
| [docs/benchmarks/day123-containerization.md](docs/benchmarks/day123-containerization.md) | Container build/smoke verification note (Day 123) |
| [docs/benchmarks/day124-k8s-validation.md](docs/benchmarks/day124-k8s-validation.md) | Local Kubernetes rollout verification note (Day 124) |
| [docs/benchmarks/day125-kubernetes-policies.md](docs/benchmarks/day125-kubernetes-policies.md) | K8s policy overlay verification note (Day 125) |
| [docs/benchmarks/day126-eks-preparation.md](docs/benchmarks/day126-eks-preparation.md) | EKS config / dry-run verification note (Day 126) |
| [docs/benchmarks/day127-logging.md](docs/benchmarks/day127-logging.md) | Structured logging / collector config verification (Day 127) |
| [docs/benchmarks/day128-monitoring.md](docs/benchmarks/day128-monitoring.md) | Monitoring rules / dashboard verification (Day 128) |
| [docs/benchmarks/day129-resilience.md](docs/benchmarks/day129-resilience.md) | Resilience failure-testing verification (Day 129) |
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
