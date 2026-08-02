# KernelQ Demo Guide (Day 130)

Local demo only — **no AWS required**.

## 10–15 minute walkthrough

### 1. Architecture (1–2 min)

Sketch: Client → FastAPI → Postgres → weighted-fair scheduler → Kafka dispatch → Go workers (pool, Redis claim, OTel) → Kafka results → Python consumer → Postgres. Mention observability (Prometheus/Grafana, structured logs) and that EKS/CloudWatch are prepared offline.

Docs: [architecture-final.md](architecture-final.md)

### 2. Start dependencies

```bash
docker compose up -d postgres zookeeper kafka redis
./infra/kafka/create-topics.sh
```

### 3. Happy path (submit → succeed)

```bash
./control_plane/scripts/smoke_full_completion.sh
```

Expect Postgres `succeeded` for a unique smoke job.

### 4. Scheduler tick (optional deeper look)

```bash
PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py
```

### 5. Worker + Kafka

With compose up, run the Go consumer (or rely on the full-completion script’s worker). Point at logs for `job execution completed` / structured JSON fields.

### 6. Duplicate replay suppression

```bash
./worker/scripts/smoke_kafka_execution_replay.sh
```

Expect `executor_calls=1`, `duplicate_executions=1`.

### 7. Metrics

```bash
PYTHONPATH=. python3 -m uvicorn control_plane.api:app --host 0.0.0.0 --port 8000
# other terminal:
curl -s http://127.0.0.1:8000/metrics/prometheus | head
docker compose up -d prometheus grafana
```

Open Grafana (local) — KernelQ MVP dashboard.

### 8. Traces

```bash
./worker/scripts/smoke_grpc_trace.sh
./worker/scripts/smoke_kafka_trace.sh
```

### 9. Kubernetes manifests / readiness

```bash
kubectl kustomize deploy/kubernetes/overlays/local | head
./worker/scripts/smoke_k8s_policies.sh
# optional if kind/cluster available:
./worker/scripts/smoke_k8s.sh
```

### 10. Resilience evidence

```bash
./worker/scripts/smoke_resilience.sh
```

### 11. Close with honesty (1 min)

- Implementation complete for portfolio scope
- Locally validated; cloud config offline
- Known gap: crash after Redis claim / before result publish — [known-limitations.md](known-limitations.md)

---

## Five-minute interview demo

1. **30s** — Architecture sentence + language split (Python decide / Go execute).
2. **90s** — `smoke_full_completion.sh` (or narrate if already run).
3. **60s** — `smoke_kafka_execution_replay.sh` (duplicates ≠ double execute).
4. **60s** — Show `/metrics/prometheus` or Grafana panel; mention OTel smokes.
5. **60s** — Limitations + roadmap (leases/watchdog; no fake 3.1× / 0.01% / 60% MTTR).

## Commands cheat sheet

| Goal | Command |
|------|---------|
| Full path | `./control_plane/scripts/smoke_full_completion.sh` |
| Duplicate delivery | `./worker/scripts/smoke_kafka_execution_replay.sh` |
| gRPC | `./worker/scripts/smoke_grpc_execute.sh` |
| Day 130 suite | `./worker/scripts/smoke_day130.sh` |
| Resilience | `./worker/scripts/smoke_resilience.sh` |
