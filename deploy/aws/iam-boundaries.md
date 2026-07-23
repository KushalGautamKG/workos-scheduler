# IAM Boundaries for KernelQ on AWS / EKS

KernelQ’s repository prepares for EKS deployment. These identities must stay separate.

## Deployment identity (CI / operator)

Used by a human or pipeline to:

- authenticate to AWS (`aws sts get-caller-identity`)
- push images to ECR
- update the EKS kubeconfig
- apply Kubernetes manifests (`kubectl apply -k`)
- inspect rollouts and events

This identity needs ECR push/pull (as needed), EKS describe/update-kubeconfig, and Kubernetes RBAC sufficient to manage KernelQ Deployments—not application data-plane AWS APIs.

## EKS node identity

Used by worker nodes (node IAM role / managed node group) to:

- join the EKS cluster
- pull container images from ECR
- interact with required AWS infrastructure for the node

Nodes should not be granted application-level permissions (e.g. broad S3/DynamoDB) “just in case.”

## Pod workload identity

Used by application Pods **only when** they need AWS API access (IRSA / EKS Pod Identity).

KernelQ today should **not** receive broad AWS API permissions merely because it runs on EKS. Prefer:

- Kafka / Redis / Postgres endpoints via network + app secrets
- no static AWS access keys in Kubernetes Secrets
- no `AWS_ACCESS_KEY_ID` baked into images

## Least-privilege rule

> No AWS permission should be attached to a KernelQ Pod unless the application requires a specific AWS API operation.

Do not commit complete production IAM policies unless they have been reviewed and tied to actual required actions.

## Related

- [README.md](README.md) — deploy prerequisites and dry-run commands
- [eks-deployment.md](../../docs/design/eks-deployment.md)
