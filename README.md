# KernelQ

Distributed job orchestration prototype: **Python control plane** (API, scheduling, Postgres state) + **Go workers** (Kafka consume/execute/publish) + **Kafka** between the planes. **Redis** (`docker compose up -d redis`, `localhost:6379`) is in the local stack for future **idempotency/deduplication** — see **[docs/design/redis-idempotency-deduplication.md](docs/design/redis-idempotency-deduplication.md)** (no app logic wired yet).

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

Provisioned dashboard **KernelQ MVP** — first metric **`kernelq_jobs_by_state`** (job counts by Postgres state). Config: [infra/prometheus/prometheus.yml](infra/prometheus/prometheus.yml). Local dev only.

## Docs

| Doc | Purpose |
|-----|---------|
| [docs/checkpoints/day90.md](docs/checkpoints/day90.md) | Day 90 checkpoint — platform status, benchmarks, limitations, roadmap |
| [docs/mvp.md](docs/mvp.md) | MVP checkpoint — demo, tests, talking points |
| [docs/architecture.md](docs/architecture.md) | System design |
| [docs/design/redis-idempotency-deduplication.md](docs/design/redis-idempotency-deduplication.md) | Redis idempotency/dedupe design (infra only; no app logic yet) |
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
