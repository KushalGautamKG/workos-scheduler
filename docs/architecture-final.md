# KernelQ Final Architecture (Day 130)

KernelQ is a **production-style portfolio implementation**: a Python control plane schedules work into Kafka; concurrent Go workers execute with bounded queues, Redis idempotency, OpenTelemetry, and structured logs; results return through Kafka to Postgres. Cloud (EKS / CloudWatch) paths are **prepared and validated offline**, not claimed as live production operation.

## End-to-end flow

```
Client
  │
  ▼
FastAPI Control Plane
  │
  ▼
PostgreSQL Job State
  │
  ▼
Weighted-Fair Scheduler
  │
  ├── Dispatch idempotency (Redis)
  ▼
Kafka Dispatch Topic (kernelq.jobs.dispatch)
  │
  ▼
Go Consumer + Bounded Worker Pool
  │
  ├── Backpressure (queue watermarks + in-memory pause/resume boundary)
  ├── Redis execution idempotency
  └── OpenTelemetry tracing
  ▼
Job Executor
  │
  ▼
Kafka Result Topic (kernelq.jobs.results)
  │
  ▼
Python Result Consumer
  │
  ├── Redis result idempotency
  ▼
PostgreSQL State Transition
```

## Cross-cutting planes

```
Prometheus ──► Grafana          (metrics; local scrape + Day 128 rules/dashboard)
Structured stdout ──► Fluent Bit ──► CloudWatch-compatible output
                                      (collector config offline-validated)
gRPC WorkerExecutionService     (internal execution + health; Kafka remains async dispatch)
Docker ──► Kubernetes ──► EKS overlay  (images + policies + deploy scripts)
```

## Technology roles (concrete)

| Technology | Role in KernelQ |
|------------|-----------------|
| **Python** | Control plane: API, scheduling, result consumption, retries/DLQ policy |
| **Go** | Worker plane: Kafka consume, pool execution, result publish, gRPC service |
| **FastAPI** | HTTP API for enqueue/query/metrics exposition |
| **PostgreSQL** | Durable job state and atomic claim transitions |
| **Kafka** | Async dispatch and result streams (at-least-once) |
| **Redis** | Fast idempotency at dispatch, execution, and result boundaries |
| **gRPC** | Internal `WorkerExecutionService` + health readiness (not primary dispatch) |
| **OpenTelemetry** | Traces across execute / gRPC / Kafka headers |
| **Prometheus** | Scrapes control-plane metrics; recording/alert rules as ConfigMaps |
| **Grafana** | Local MVP dashboard + Day 128 monitoring dashboard JSON |
| **Docker** | Multi-stage images for worker and control plane |
| **Kubernetes** | Local + production Kustomize overlays, PDBs, security contexts |
| **EKS** | Overlay + ECR publish/deploy/rollback scripts (prep) |
| **Fluent Bit** | DaemonSet config to ship container JSON logs |
| **CloudWatch-oriented logging** | Output plugin placeholders; IAM boundary documented |

## Validated locally vs prepared offline

| Integration | Status |
|-------------|--------|
| Postgres, Kafka, Redis, full happy path | **Locally verified** (smokes + benchmarks) |
| Worker pool, backpressure signals, retries/DLQ | **Locally verified** |
| Redis idempotency + Kafka replay | **Locally verified** |
| gRPC execute/health + OTel spans | **Locally verified** |
| Prometheus scrape + Grafana (compose) | **Locally verified** (dev) |
| Structured logging + Fluent Bit / CloudWatch config | **Offline config validated** |
| EKS/ECR workflow | **Offline config validated** (not a live AWS cluster claim) |
| Production SLO compliance / multi-region | **Not claimed** |

See also: [capability-evidence-matrix.md](capability-evidence-matrix.md), [production-readiness.md](production-readiness.md), [known-limitations.md](known-limitations.md).
