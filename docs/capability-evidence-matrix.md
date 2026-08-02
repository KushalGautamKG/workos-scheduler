# Capability and Evidence Matrix (Day 130)

Status legend:

| Status | Meaning |
|--------|---------|
| **Implemented and verified** | Code + tests/smokes support the claim |
| **Implemented with documented limitation** | Works as designed; known gap called out |
| **Configuration validated offline** | Manifests/scripts render and smoke without live cloud |
| **Designed only** | Documented; not implemented |

| Capability | Implementation | Tests | Smoke or benchmark | Documentation | Status |
|------------|----------------|-------|--------------------|---------------|--------|
| Weighted-fair scheduling | Control-plane scheduler / composed policy | pytest scheduler tests | scheduler throughput benches | architecture, mvp | Implemented and verified |
| Atomic PostgreSQL job claiming | `JobRepository` / tick runner | pytest | full completion + scheduler benches | architecture | Implemented and verified |
| Kafka dispatch | Python producer → `kernelq.jobs.dispatch` | pytest + Go parse tests | full completion, worker benches | ADR-0002, architecture | Implemented and verified |
| Concurrent worker pool | `WorkerPool` | Go tests | worker throughput, queue saturation | worker README | Implemented and verified |
| Bounded worker queue | `QueueCapacity` | Go tests | `smoke_queue_saturation.sh` | design notes | Implemented and verified |
| Local backpressure | Watermark policy + in-memory pause/resume | Go tests | `smoke_backpressure_config.sh` | architecture | Implemented with documented limitation |
| Retry scheduling | `retry_scheduled` + scanner | pytest | `smoke_retry_requeue.sh` | runbooks, mvp | Implemented and verified |
| Retry exhaustion | → `dead_lettered` | pytest | `smoke_retry_exhaustion.sh` | runbooks | Implemented and verified |
| DLQ | Go dead-letter producer | Go tests | consumer error path | architecture | Implemented and verified |
| Worker result processing | Result consumer → Postgres | pytest | full completion / e2e bench | mvp | Implemented and verified |
| Dispatch idempotency | Redis/memory `dispatch_key` | pytest | tick logs / smokes | redis-idempotency design | Implemented and verified |
| Execution idempotency | Redis/memory `execution:` key | Go tests | `smoke_worker_execution_idempotency.sh`, Kafka replay | worker-execution-idempotency | Implemented with documented limitation |
| Result idempotency | `worker_result_key` | pytest | result consumer paths | redis-idempotency design | Implemented and verified |
| Duplicate replay handling | Claim skip + metrics/logs | Go/pytest | `smoke_kafka_execution_replay.sh` | day114 bench, duplicate runbook | Implemented and verified |
| Prometheus metrics | `/metrics/prometheus` | pytest | local compose scrape | deploy.md, monitoring | Implemented and verified |
| Grafana dashboards | MVP + Day 128 JSON | smoke_dashboard | local Grafana | grafana README | Implemented and verified (local) |
| gRPC service | `WorkerExecutionService` + health | Go tests | `smoke_grpc_execute.sh`, health | grpc design | Implemented and verified |
| gRPC tracing | otelgrpc | Go tests | `smoke_grpc_trace.sh` | grpc-tracing | Implemented and verified |
| Kafka tracing | W3C headers | Go tests | `smoke_kafka_trace.sh` | kafka-tracing | Implemented and verified |
| Structured logging | Go slog + Python context | Go/pytest | `smoke_logging.sh` | structured-logging | Implemented and verified |
| Fluent Bit configuration | DaemonSet + ConfigMap | `smoke_cloudwatch_config.sh` | kustomize render | structured-logging | Configuration validated offline |
| Docker images | Multi-stage Dockerfiles | `smoke_container.sh` | day123 note | containerization | Implemented and verified |
| Local Kubernetes deployment | overlays/local | `smoke_k8s.sh` (when cluster) | day124 | local-kubernetes | Implemented and verified (when cluster) |
| Kubernetes production policies | overlays/production | `smoke_k8s_policies.sh` | day125 | k8s production policies | Configuration validated offline |
| EKS deployment preparation | overlays/eks + scripts | `smoke_eks_config.sh` | day126 | eks-deployment | Configuration validated offline |
| Monitoring alerts and runbooks | Prometheus rules + docs/runbooks | `smoke_monitoring.sh` | day128 | monitoring | Configuration validated offline |
| Resilience testing | faults + smokes | Go faults tests | `smoke_resilience.sh` | resilience-testing | Implemented and verified (local) |
| Execution lease / watchdog recovery | — | educational gap smoke | execution-recovery design | Designed only |
| Live CloudWatch ingestion | — | offline config only | cloudwatch-logs.md | Configuration validated offline |
| Production SLO compliance | Proposed in monitoring.md | — | — | Designed only |

**Do not treat EKS or CloudWatch as live production deployments** based on this matrix.
