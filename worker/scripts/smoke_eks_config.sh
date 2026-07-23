#!/usr/bin/env bash
#
# Offline EKS configuration smoke (Day 126).
# No AWS credentials, ECR, EKS cluster, or network required.
#
# Run from repository root:
#   ./worker/scripts/smoke_eks_config.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d deploy/kubernetes/overlays/eks ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

ROOT_DIR="$(pwd)"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-eks-config-smoke-$$"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"

AWS_ACCOUNT_ID="123456789012"
AWS_REGION="us-east-1"
EKS_CLUSTER_NAME="kernelq-example"
IMAGE_TAG="0123456789abcdef0123456789abcdef01234567"

WORKER_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/kernelq-worker:${IMAGE_TAG}"
CONTROL_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/kernelq-control-plane:${IMAGE_TAG}"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_eks_config success=false" >&2
  exit 1
}

cleanup() {
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"
command -v git >/dev/null 2>&1 || fail "git is required"
mkdir -p "${TMP_DIR}"

git status --short >"${STATUS_BEFORE}"

echo "==> Rendering tracked EKS overlay..."
kubectl kustomize deploy/kubernetes/overlays/eks >"${TMP_DIR}/eks-raw.yaml" \
  || fail "kustomize eks failed"

grep -Eq 'replicas:[[:space:]]*2' "${TMP_DIR}/eks-raw.yaml" || fail "EKS overlay missing replicas: 2"
grep -Eq 'kind:[[:space:]]*PodDisruptionBudget' "${TMP_DIR}/eks-raw.yaml" || fail "EKS overlay missing PDBs"
grep -Eq 'runAsNonRoot:[[:space:]]*true' "${TMP_DIR}/eks-raw.yaml" || fail "missing runAsNonRoot"
grep -Eq 'maxUnavailable:[[:space:]]*0' "${TMP_DIR}/eks-raw.yaml" || fail "missing rolling-update maxUnavailable: 0"
grep -Eq 'topologySpreadConstraints:' "${TMP_DIR}/eks-raw.yaml" || fail "missing topology spread"

echo "==> Injecting fake ECR images via ephemeral wrapper..."
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

kubectl kustomize "${WORK}" >"${TMP_DIR}/eks-injected.yaml" \
  || fail "kustomize with fake images failed"

grep -Fq "${WORKER_URI}" "${TMP_DIR}/eks-injected.yaml" || fail "missing worker ECR URI"
grep -Fq "${CONTROL_URI}" "${TMP_DIR}/eks-injected.yaml" || fail "missing control-plane ECR URI"
grep -Fq "${AWS_ACCOUNT_ID}" "${TMP_DIR}/eks-injected.yaml" || fail "missing fake account id"
grep -Fq "${AWS_REGION}" "${TMP_DIR}/eks-injected.yaml" || fail "missing fake region"
grep -Fq "${IMAGE_TAG}" "${TMP_DIR}/eks-injected.yaml" || fail "missing immutable fake SHA"
grep -Fq 'latest' "${TMP_DIR}/eks-injected.yaml" && fail "must not use latest tag" || true

assert_kind_name() {
  local kind="$1"
  local name="$2"
  grep -E "kind:[[:space:]]*${kind}" -A20 "${TMP_DIR}/eks-injected.yaml" | grep -Eq "name:[[:space:]]*${name}" \
    || fail "missing ${kind}/${name}"
}

# Simpler presence checks
grep -Eq 'name:[[:space:]]*kernelq-worker' "${TMP_DIR}/eks-injected.yaml" || fail "missing kernelq-worker"
grep -Eq 'name:[[:space:]]*kernelq-control-plane' "${TMP_DIR}/eks-injected.yaml" || fail "missing kernelq-control-plane"
grep -Eq 'kind:[[:space:]]*Service' "${TMP_DIR}/eks-injected.yaml" || fail "missing Services"
PDB_COUNT="$(grep -c 'kind: PodDisruptionBudget' "${TMP_DIR}/eks-injected.yaml" || true)"
[[ "${PDB_COUNT}" -ge 2 ]] || fail "expected 2 PDBs"
grep -Eq 'readOnlyRootFilesystem:[[:space:]]*true' "${TMP_DIR}/eks-injected.yaml" || fail "missing RO rootfs"
grep -Eq 'cpu:[[:space:]]*100m' "${TMP_DIR}/eks-injected.yaml" || fail "missing CPU requests"
grep -Eq 'memory:[[:space:]]*512Mi' "${TMP_DIR}/eks-injected.yaml" || fail "missing memory limits"
grep -Eq 'grpc:' "${TMP_DIR}/eks-injected.yaml" || fail "missing gRPC probes"
grep -Eq 'httpGet:' "${TMP_DIR}/eks-injected.yaml" || fail "missing HTTP probes"
grep -Eq 'terminationGracePeriodSeconds:[[:space:]]*30' "${TMP_DIR}/eks-injected.yaml" \
  || fail "missing terminationGracePeriodSeconds"

echo "==> Running publish_ecr.sh dry-run..."
DRY_RUN=true \
  AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}" \
  AWS_REGION="${AWS_REGION}" \
  IMAGE_TAG="${IMAGE_TAG}" \
  ./worker/scripts/publish_ecr.sh | tee "${TMP_DIR}/publish.out" \
  || fail "publish dry-run failed"
grep -Fq "event=ecr_publish_dry_run success=true" "${TMP_DIR}/publish.out" \
  || fail "missing ecr_publish_dry_run success"

echo "==> Running deploy_eks.sh dry-run..."
DRY_RUN=true \
  AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}" \
  AWS_REGION="${AWS_REGION}" \
  EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME}" \
  IMAGE_TAG="${IMAGE_TAG}" \
  ./worker/scripts/deploy_eks.sh | tee "${TMP_DIR}/deploy.out" \
  || fail "deploy dry-run failed"
grep -Fq "event=eks_deploy_dry_run success=true" "${TMP_DIR}/deploy.out" \
  || fail "missing eks_deploy_dry_run success"

echo "==> Running rollback_eks.sh dry-run..."
DRY_RUN=true \
  AWS_REGION="${AWS_REGION}" \
  EKS_CLUSTER_NAME="${EKS_CLUSTER_NAME}" \
  KERNELQ_DEPLOYMENT=all \
  ./worker/scripts/rollback_eks.sh | tee "${TMP_DIR}/rollback.out" \
  || fail "rollback dry-run failed"
grep -Fq "event=eks_rollback_dry_run success=true" "${TMP_DIR}/rollback.out" \
  || fail "missing eks_rollback_dry_run success"

git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  echo "=== git status before ===" >&2
  cat "${STATUS_BEFORE}" >&2
  echo "=== git status after ===" >&2
  cat "${STATUS_AFTER}" >&2
  fail "tracked working tree changed during smoke"
fi

echo "PASS: EKS configuration smoke succeeded"
echo "event=smoke_eks_config success=true"
