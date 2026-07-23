#!/usr/bin/env bash
#
# Deploy KernelQ to EKS using the EKS Kustomize overlay (Day 126).
# Injects ECR image URIs via an ephemeral wrapper — does not mutate tracked files.
#
# Required: AWS_REGION, AWS_ACCOUNT_ID, EKS_CLUSTER_NAME
# Optional: IMAGE_TAG (default git HEAD), KERNELQ_NAMESPACE, DRY_RUN, AUTO_APPROVE
#
# Run from repository root.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

DRY_RUN="${DRY_RUN:-false}"
AUTO_APPROVE="${AUTO_APPROVE:-false}"
AWS_REGION="${AWS_REGION:-}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-}"
EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME:-}"
IMAGE_TAG="${IMAGE_TAG:-}"
KERNELQ_NAMESPACE="${KERNELQ_NAMESPACE:-kernelq}"
TMP_DIR=""
RENDERED=""

fail() {
  echo "FAIL: $*" >&2
  if [[ "${DRY_RUN}" == "true" ]]; then
    echo "event=eks_deploy_dry_run success=false" >&2
  else
    echo "event=eks_deploy success=false" >&2
  fi
  collect_diagnostics || true
  exit 1
}

cleanup() {
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
}

collect_diagnostics() {
  [[ "${DRY_RUN}" == "true" ]] && return 0
  kubectl get deploy,rs,pod,svc,pdb,events -n "${KERNELQ_NAMESPACE}" -o wide 2>&1 || true
  local pod
  for pod in $(kubectl -n "${KERNELQ_NAMESPACE}" get pods -o name 2>/dev/null || true); do
    kubectl -n "${KERNELQ_NAMESPACE}" describe "${pod}" 2>&1 || true
    kubectl -n "${KERNELQ_NAMESPACE}" logs "${pod}" --all-containers=true --tail=200 2>&1 || true
    kubectl -n "${KERNELQ_NAMESPACE}" logs "${pod}" --all-containers=true --previous --tail=100 2>&1 || true
  done
}

trap cleanup EXIT INT TERM

[[ -n "${AWS_REGION}" ]] || fail "AWS_REGION is required"
[[ -n "${AWS_ACCOUNT_ID}" ]] || fail "AWS_ACCOUNT_ID is required"
[[ -n "${EKS_CLUSTER_NAME}" ]] || fail "EKS_CLUSTER_NAME is required"
[[ "${AWS_ACCOUNT_ID}" =~ ^[0-9]{12}$ ]] || fail "AWS_ACCOUNT_ID must be 12 digits"

command -v git >/dev/null 2>&1 || fail "git is required"
command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"

if [[ -z "${IMAGE_TAG}" ]]; then
  IMAGE_TAG="$(git rev-parse HEAD)"
fi
[[ -n "${IMAGE_TAG}" ]] || fail "IMAGE_TAG is empty"
[[ "${IMAGE_TAG}" =~ ^[0-9a-fA-F]{7,64}$ ]] || fail "IMAGE_TAG malformed: ${IMAGE_TAG}"
[[ "${IMAGE_TAG}" != "latest" ]] || fail "refusing to deploy tag 'latest'"

WORKER_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/kernelq-worker:${IMAGE_TAG}"
CONTROL_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/kernelq-control-plane:${IMAGE_TAG}"

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/kernelq-eks-deploy.XXXXXX")"
RENDERED="${TMP_DIR}/rendered.yaml"
WORK="${TMP_DIR}/work"
mkdir -p "${WORK}"
cp -a "${ROOT_DIR}/deploy/kubernetes" "${WORK}/kubernetes"

cat >"${WORK}/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
resources:
  - kubernetes/overlays/eks
images:
  - name: kernelq-worker
    newName: ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/kernelq-worker
    newTag: "${IMAGE_TAG}"
  - name: kernelq-control-plane
    newName: ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/kernelq-control-plane
    newTag: "${IMAGE_TAG}"
EOF

kubectl kustomize "${WORK}" >"${RENDERED}" || fail "failed to render EKS overlay with images"

grep -Fq "${WORKER_URI}" "${RENDERED}" || fail "rendered YAML missing worker image ${WORKER_URI}"
grep -Fq "${CONTROL_URI}" "${RENDERED}" || fail "rendered YAML missing control-plane image ${CONTROL_URI}"
grep -Eq 'kind:[[:space:]]*PodDisruptionBudget' "${RENDERED}" || fail "PDBs missing from render"
grep -Eq 'replicas:[[:space:]]*2' "${RENDERED}" || fail "expected replicas: 2 preserved from production"

KUBE_CONTEXT="<unresolved>"
if [[ "${DRY_RUN}" != "true" ]]; then
  command -v aws >/dev/null 2>&1 || fail "aws CLI is required for real deploy"
  RESOLVED_ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
  [[ "${RESOLVED_ACCOUNT}" == "${AWS_ACCOUNT_ID}" ]] \
    || fail "AWS account mismatch: expected ${AWS_ACCOUNT_ID}, got ${RESOLVED_ACCOUNT:-<none>}"

  aws eks update-kubeconfig --region "${AWS_REGION}" --name "${EKS_CLUSTER_NAME}" >/dev/null \
    || fail "aws eks update-kubeconfig failed"
  KUBE_CONTEXT="$(kubectl config current-context 2>/dev/null || true)"
  [[ -n "${KUBE_CONTEXT}" ]] || fail "Kubernetes context could not be resolved"
  echo "${KUBE_CONTEXT}" | grep -Fq "${EKS_CLUSTER_NAME}" \
    || fail "kubectl context '${KUBE_CONTEXT}' does not appear to match cluster ${EKS_CLUSTER_NAME}"
else
  KUBE_CONTEXT="<dry-run:not-connected>"
fi

echo "AWS account:        ${AWS_ACCOUNT_ID}"
echo "AWS region:         ${AWS_REGION}"
echo "EKS cluster:        ${EKS_CLUSTER_NAME}"
echo "Kubernetes context: ${KUBE_CONTEXT}"
echo "Namespace:          ${KERNELQ_NAMESPACE}"
echo "Worker image:       ${WORKER_URI}"
echo "Control image:      ${CONTROL_URI}"

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "==> Dry-run only — not applying to cluster"
  echo "event=eks_deploy_dry_run success=true"
  exit 0
fi

if [[ "${AUTO_APPROVE}" != "true" ]]; then
  echo -n "Type 'deploy' to continue: "
  read -r CONFIRM
  [[ "${CONFIRM}" == "deploy" ]] || fail "confirmation rejected"
fi

kubectl apply --validate=false -f "${RENDERED}" || fail "kubectl apply failed"

kubectl -n "${KERNELQ_NAMESPACE}" rollout status deployment/kernelq-worker --timeout=5m \
  || fail "worker rollout failed"
kubectl -n "${KERNELQ_NAMESPACE}" rollout status deployment/kernelq-control-plane --timeout=5m \
  || fail "control-plane rollout failed"

kubectl -n "${KERNELQ_NAMESPACE}" get pods
kubectl -n "${KERNELQ_NAMESPACE}" get services
kubectl -n "${KERNELQ_NAMESPACE}" get pdb

DEPLOYED_WORKER="$(kubectl -n "${KERNELQ_NAMESPACE}" get deployment kernelq-worker \
  -o jsonpath='{.spec.template.spec.containers[0].image}')"
DEPLOYED_CONTROL="$(kubectl -n "${KERNELQ_NAMESPACE}" get deployment kernelq-control-plane \
  -o jsonpath='{.spec.template.spec.containers[0].image}')"

[[ "${DEPLOYED_WORKER}" == "${WORKER_URI}" ]] \
  || fail "worker image mismatch: got ${DEPLOYED_WORKER}, want ${WORKER_URI}"
[[ "${DEPLOYED_CONTROL}" == "${CONTROL_URI}" ]] \
  || fail "control-plane image mismatch: got ${DEPLOYED_CONTROL}, want ${CONTROL_URI}"

echo "event=eks_deploy success=true"
