# Local Kubernetes Deployment

Day 124 validates KernelQ on a local Kubernetes cluster (kind, minikube, or Docker Desktop) using the Day 123 manifests.

**Interview sound bite:** *“Deployments keep Pods alive; Services give them a stable name; readiness waits for SERVING before traffic.”*

---

## Before (Day 123)

```
Docker → Container (smoke on Docker Engine)
```

## After (Day 124)

```
Deployment → ReplicaSet → Pod → Service
                ↑
         ConfigMap / Secret (env)
```

---

## Objects

| Object | Role |
|--------|------|
| **Deployment** | Desired replicas, rolling update, restart failed Pods |
| **ReplicaSet** | Owned by Deployment; maintains Pod set |
| **Pod** | Running container(s); ephemeral IP |
| **Service** | Stable DNS + ClusterIP (even with one replica) |
| **ConfigMap** | Non-secret config (`KERNELQ_GRPC_ADDR`, OTel, …) |
| **Secret** | Credentials / connection URLs (example only in repo) |

## Readiness

Worker probes use Day 118 **`grpc.health.v1`**. Until overall health is **SERVING**, the Pod is not Ready and the Service does not send traffic.

## Graceful termination

`terminationGracePeriodSeconds: 30` plus the process SIGTERM handler: mark **NOT_SERVING**, then graceful gRPC stop.

## Apply

```bash
kubectl kustomize deploy/kubernetes/overlays/local
kubectl apply -k deploy/kubernetes/overlays/local
./worker/scripts/smoke_k8s.sh
./worker/scripts/smoke_k8s_policies.sh
```

## Deferred

| Item | When |
|------|------|
| EKS / cloud cluster | Later |
| Helm / Ingress / HPA | Later |

---

## Related

- [containerization.md](containerization.md)
- [grpc-lifecycle.md](grpc-lifecycle.md)
- [day124-k8s-validation.md](../benchmarks/day124-k8s-validation.md)
