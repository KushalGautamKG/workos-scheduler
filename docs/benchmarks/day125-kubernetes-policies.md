# Day 125 — Kubernetes Production Policies (Verification Note)

Functional and configuration validation only — **not** a performance benchmark.

The resource request/limit values are **initial deployment defaults** and were **not** derived from a production load test. Do not interpret them as capacity or throughput claims.

## What was verified

| Check | Result |
|-------|--------|
| `kubectl kustomize` base | Pass |
| `kubectl kustomize` local overlay | Pass |
| `kubectl kustomize` production overlay | Pass |
| Client-side dry-run apply (local + production) | Pass |
| Production: replicas 2, resources, security, spread, PDBs, rolling update | Pass |
| Local k8s regression (`smoke_k8s.sh`) | Pass |

## Smokes

```bash
./worker/scripts/smoke_k8s_policies.sh
# event=smoke_k8s_policies success=true

./worker/scripts/smoke_k8s.sh
# event=smoke_k8s success=true
```

## Related

- [kubernetes-production-policies.md](../design/kubernetes-production-policies.md)
- [day124-k8s-validation.md](day124-k8s-validation.md)
