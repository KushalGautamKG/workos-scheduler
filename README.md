# KernelQ

Distributed job orchestration prototype: **Python control plane** (API, scheduling, Postgres state) + **Go workers** (Kafka consume/execute/publish) + **Kafka** between the planes.

## MVP Status

KernelQ has reached an **MVP checkpoint**. See **[docs/mvp.md](docs/mvp.md)** for capabilities, demo commands, limitations, and resume talking points.

**Happy path (smoke-tested):**

```
queued job → scheduler → Kafka → Go worker → result event → Postgres SUCCEEDED
```

**Retry and dead-letter flows** are also smoke-tested: `retryable_failure` → `RETRY_SCHEDULED` → requeue → dispatch; exhaustion → `DEAD_LETTERED`; operator inspection and manual requeue.

## Quick start

```bash
docker compose up -d postgres zookeeper kafka
./infra/kafka/create-topics.sh
./control_plane/scripts/smoke_full_completion.sh
```

## Docs

| Doc | Purpose |
|-----|---------|
| [docs/mvp.md](docs/mvp.md) | MVP checkpoint — demo, tests, talking points |
| [docs/architecture.md](docs/architecture.md) | System design |
| [docs/deploy.md](docs/deploy.md) | Local setup and smoke tests |
| [docs/runbooks.md](docs/runbooks.md) | Operational runbooks |
| [control_plane/README.md](control_plane/README.md) | Control plane details |
| [worker/README.md](worker/README.md) | Go worker details |

## Tests

```bash
python3 -m pytest control_plane/tests
cd worker && go test ./...
```
