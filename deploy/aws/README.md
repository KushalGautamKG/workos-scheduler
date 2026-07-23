# KernelQ AWS / EKS Deployment Prep

This directory documents **deployment preparation**. It does **not** prove that a production EKS cluster exists or that KernelQ is running in AWS.

## Prerequisites (real deploy)

- AWS CLI authenticated
- Docker running
- `kubectl` installed (with `kubectl kustomize`)
- Existing EKS cluster
- ECR create/push access (for publishing)
- EKS deploy access (for apply/rollout)

## Environment variables

| Variable | Required | Notes |
|----------|----------|--------|
| `AWS_ACCOUNT_ID` | yes (real) | 12-digit account; must match `sts get-caller-identity` |
| `AWS_REGION` | yes | e.g. `us-east-1` |
| `EKS_CLUSTER_NAME` | yes (deploy/rollback) | Target cluster |
| `IMAGE_TAG` | optional | Default: full `git rev-parse HEAD` (immutable) |
| `KERNELQ_NAMESPACE` | optional | Default: `kernelq` |
| `DRY_RUN` | optional | `true` = validate only, no AWS mutations |
| `AUTO_APPROVE` | optional | `true` skips interactive `deploy` confirmation |
| `CREATE_ECR_REPOS` | optional | `true` may create missing ECR repos (publish only) |
| `KERNELQ_DEPLOYMENT` | rollback | `kernelq-worker` \| `kernelq-control-plane` \| `all` |
| `ROLLBACK_REVISION` | optional | `kubectl rollout undo --to-revision` |

**Never use `latest` as the production image tag.**

## Validation-only (no AWS required)

```bash
# From repository root
DRY_RUN=true \
  AWS_ACCOUNT_ID=123456789012 \
  AWS_REGION=us-east-1 \
  ./worker/scripts/publish_ecr.sh

DRY_RUN=true \
  AWS_ACCOUNT_ID=123456789012 \
  AWS_REGION=us-east-1 \
  EKS_CLUSTER_NAME=kernelq-example \
  IMAGE_TAG=0123456789abcdef0123456789abcdef01234567 \
  ./worker/scripts/deploy_eks.sh

DRY_RUN=true \
  AWS_REGION=us-east-1 \
  EKS_CLUSTER_NAME=kernelq-example \
  KERNELQ_DEPLOYMENT=all \
  ./worker/scripts/rollback_eks.sh

./worker/scripts/smoke_eks_config.sh
```

## Real publishing / deploy (requires AWS)

1. Create ECR repositories per [ecr-repositories.json](ecr-repositories.json) (or `CREATE_ECR_REPOS=true` once).
2. `AWS_ACCOUNT_ID=… AWS_REGION=… ./worker/scripts/publish_ecr.sh`
3. `AWS_ACCOUNT_ID=… AWS_REGION=… EKS_CLUSTER_NAME=… ./worker/scripts/deploy_eks.sh`  
   Confirm with exact word: `deploy` (unless `AUTO_APPROVE=true`).
4. Rollback: `./worker/scripts/rollback_eks.sh` with `KERNELQ_DEPLOYMENT=…`

See [iam-boundaries.md](iam-boundaries.md) for deployment vs node vs Pod identities.

## Related

- Overlay: `deploy/kubernetes/overlays/eks`
- Design: [eks-deployment.md](../../docs/design/eks-deployment.md)
