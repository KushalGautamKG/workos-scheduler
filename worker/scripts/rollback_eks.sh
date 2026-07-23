#!/usr/bin/env bash
#
# Roll back KernelQ Deployments on EKS to a prior revision (Day 126).
#
# Required: AWS_REGION, EKS_CLUSTER_NAME, KERNELQ_DEPLOYMENT
# Optional: ROLLBACK_REVISION, DRY_RUN, AUTO_APPROVE, KERNELQ_NAMESPACE
#
# KERNELQ_DEPLOYMENT: kernelq-worker | kernelq-control-plane | all

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

DRY_RUN="${DRY_RUN:-false}"
AUTO_APPROVE="${AUTO_APPROVE:-false}"
AWS_REGION="${AWS_REGION:-}"
EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME:-}"
KERNELQ_DEPLOYMENT="${KERNELQ_DEPLOYMENT:-}"
KERNELQ_NAMESPACE="${KERNELQ_NAMESPACE:-kernelq}"
ROLLBACK_REVISION="${ROLLBACK_REVISION:-}"

fail() {
  echo "FAIL: $*" >&2
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "event=eks_rollback_dry_run success=false" >&2
  else
    echo "event=eks_rollback success=false" >&2
  fi
  exit 1
}

[[ -n "${AWS_REGION}" ]] || fail "AWS_REGION is required"
[[ -n "${EKS_CLUSTER_NAME}" ]] || fail "EKS_CLUSTER_NAME is required"
[[ -n "${KERNELQ_DEPLOYMENT}" ]] || fail "KERNELQ_DEPLOYMENT is required"

case "${KERNELQ_DEPLOYMENT}" in
  kernelq-worker|kernelq-control-plane|all) ;;
  *) fail "KERNELQ_DEPLOYMENT must be kernelq-worker|kernelq-control-plane|all" ;;
esac

targets=()
if [[ "${KERNELQ_DEPLOYMENT}" == "all" ]]; then
  targets=(kernelq-worker kernelq-control-plane)
else
  targets=("${KERNELQ_DEPLOYMENT}")
fi

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "==> Rollback dry-run plan"
  echo "AWS_REGION=${AWS_REGION}"
  echo "EKS_CLUSTER_NAME=${EKS_CLUSTER_NAME}"
  echo "KERNELQ_NAMESPACE=${KERNELQ_NAMESPACE}"
  for d in "${targets[@]}"; do
    if [[ -n "${ROLLBACK_REVISION}" ]]; then
      echo "kubectl -n ${KERNELQ_NAMESPACE} rollout undo deployment/${d} --to-revision=${ROLLBACK_REVISION}"
    else
      echo "kubectl -n ${KERNELQ_NAMESPACE} rollout undo deployment/${d}"
    fi
  done
  echo "event=eks_rollback_dry_run success=true"
  exit 0
fi

command -v aws >/dev/null 2>&1 || fail "aws CLI is required"
command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"

aws sts get-caller-identity >/dev/null || fail "unable to resolve AWS identity"
aws eks update-kubeconfig --region "${AWS_REGION}" --name "${EKS_CLUSTER_NAME}" >/dev/null \
  || fail "update-kubeconfig failed"
CTX="$(kubectl config current-context 2>/dev/null || true)"
[[ -n "${CTX}" ]] || fail "no kubectl context"
echo "${CTX}" | grep -Fq "${EKS_CLUSTER_NAME}" \
  || fail "context '${CTX}' does not match cluster ${EKS_CLUSTER_NAME}"

for d in "${targets[@]}"; do
  echo "==> History for ${d}"
  kubectl -n "${KERNELQ_NAMESPACE}" rollout history "deployment/${d}" || true
  echo -n "Current image: "
  kubectl -n "${KERNELQ_NAMESPACE}" get "deployment/${d}" \
    -o jsonpath='{.spec.template.spec.containers[0].image}'
  echo
done

if [[ "${AUTO_APPROVE}" != "true" ]]; then
  echo -n "Type 'rollback' to continue: "
  read -r CONFIRM
  [[ "${CONFIRM}" == "rollback" ]] || fail "confirmation rejected"
fi

for d in "${targets[@]}"; do
  if [[ -n "${ROLLBACK_REVISION}" ]]; then
    kubectl -n "${KERNELQ_NAMESPACE}" rollout undo "deployment/${d}" \
      --to-revision="${ROLLBACK_REVISION}" \
      || fail "rollback failed for ${d}"
  else
    kubectl -n "${KERNELQ_NAMESPACE}" rollout undo "deployment/${d}" \
      || fail "rollback failed for ${d}"
  fi
  kubectl -n "${KERNELQ_NAMESPACE}" rollout status "deployment/${d}" --timeout=5m \
    || fail "rollout status failed for ${d}"
  echo -n "Restored image (${d}): "
  kubectl -n "${KERNELQ_NAMESPACE}" get "deployment/${d}" \
    -o jsonpath='{.spec.template.spec.containers[0].image}'
  echo
done

echo "event=eks_rollback success=true"
