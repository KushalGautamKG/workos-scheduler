# CloudWatch Logs IAM boundary for the Fluent Bit collector (Day 127)

## Scope

This document describes the **collector workload identity** permission boundary for
shipping KernelQ container logs to Amazon CloudWatch Logs via Fluent Bit on EKS.

It does **not** grant permissions to KernelQ Worker or Control Plane Pods.

## Required operations (collector identity)

The Fluent Bit identity may require operations equivalent to:

- `logs:CreateLogStream`
- `logs:DescribeLogStreams`
- `logs:PutLogEvents`

Depending on whether log groups are pre-created, it may also require:

- `logs:CreateLogGroup`

**Prefer pre-created log groups** so the runtime role has fewer permissions.

Suggested log group: `/kernelq/eks/application`

## Explicit non-goals

| Must | Must not |
|------|----------|
| Belong to the Fluent Bit / collector service account (`kernelq-fluent-bit`) | Belong to KernelQ Worker or Control Plane Pods |
| Use EKS Pod Identity or IRSA (workload identity) | Mount static AWS access keys in Pods |
| Restrict to the intended log group ARN when the account/region are known | Ship an unreviewed wildcard policy as production-ready |

## Deferred until account specifics are known

Exact IAM policy JSON generation is deferred until:

- AWS account ID
- Region
- Log-group lifecycle (pre-created vs runtime create)
- Whether Pod Identity or IRSA is chosen

Do **not** treat a sample `Resource: "*"` policy as production-ready.

## Offline validation

CloudWatch configuration is validated offline (Kustomize render + smoke scripts).
That validation does **not** prove real ingestion, delivery guarantees, retention
compliance, or production cost characteristics.
