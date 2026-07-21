# Containerization Foundation

Day 123 packages KernelQ as immutable container images and Kubernetes manifests. Cluster apply is Day 124.

**Interview sound bite:** *“Same image everywhere—config via env, non-root runtime, and readiness from the app’s health signal, not just PID alive.”*

---

## Before

```
Source → go run / uvicorn
```

## After

```
Source → Docker multi-stage build → Image → Container → Kubernetes Deployment
```

---

## Design choices

| Topic | Choice |
|-------|--------|
| **Immutable images** | Build once; promote the same digest across envs |
| **Configuration** | ConfigMap + Secret (env) — never bake secrets into layers |
| **Non-root** | UID 65532 in Docker and Pod `securityContext` |
| **Worker health** | Day 118 `grpc.health.v1` (`SERVING` / `NOT_SERVING`); native gRPC probes (exec fallback documented) |
| **Control plane health** | Existing `GET /health` |
| **Stateless images** | Redis / Postgres / Kafka stay external |
| **Deployment vs Pod** | Deployments own replicas, rollouts, restarts |

Worker image default entrypoint: **`grpc-server`** (probe-ready). Kafka **`consumer`** binary ships in the same image for a future command override.

---

## Layout

```
deploy/docker/          Dockerfiles + dockerignore
deploy/kubernetes/      Namespace, ConfigMap, Secret example, Deployments, Services, kustomization
```

---

## Deferred

| Item | When |
|------|------|
| Apply to a local/remote cluster | Day 124 |
| Helm charts | Later |
| HPA / Ingress / ServiceMonitor | Later |

---

## Related

- [grpc-lifecycle.md](grpc-lifecycle.md) — readiness model
- [day123-containerization.md](../benchmarks/day123-containerization.md)
- `./worker/scripts/smoke_container.sh`
