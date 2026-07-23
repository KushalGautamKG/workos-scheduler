# EKS Deployment Preparation

Day 126 prepares KernelQ for Amazon EKS using immutable ECR images. This is **deployment preparation**, not evidence that a production EKS cluster has been provisioned.

**Interview sound bite:** *“Build once with a Git SHA tag in ECR, inject those URIs through an ephemeral Kustomize wrapper, verify account/region/context before apply, and roll back by revision—not by rebuilding.”*

---

## Before Day 126

```
Production Kubernetes Overlay
            │
       Generic Images (:local)
```

## After Day 126

```
Git Commit
    │
Docker Images
    │
Immutable ECR Tags (<sha>)
    │
EKS Overlay (→ production policies)
    │
Validated Rollout
    │
Rollback Procedure
```

---

## Topics

| Topic | Approach |
|-------|----------|
| **ECR boundaries** | Separate repos: `kernelq-worker`, `kernelq-control-plane`; immutable tags; scan on push |
| **Immutable tags** | Full Git SHA — never deploy `latest` |
| **EKS overlay** | `overlays/eks` → `overlays/production` → `base` |
| **Image injection** | Ephemeral wrapper Kustomization (no tracked-file mutation) |
| **Context safety** | Verify account, region, `update-kubeconfig`, context contains cluster name |
| **Rollout** | `rollout status` + confirm Deployment image matches requested tag |
| **Rollback** | `kubectl rollout undo` (± revision) to a known artifact |
| **IAM** | Deployment identity ≠ node identity ≠ Pod identity ([iam-boundaries.md](../../deploy/aws/iam-boundaries.md)) |

## Deferred

- Infrastructure-as-code for the EKS cluster itself
- Managed Kafka / Redis / Postgres on AWS
- Real cloud deployment verification

---

## Related

- [deploy/aws/README.md](../../deploy/aws/README.md)
- [day126-eks-preparation.md](../benchmarks/day126-eks-preparation.md)
- `./worker/scripts/smoke_eks_config.sh`
