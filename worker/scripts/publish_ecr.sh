#!/usr/bin/env bash
#
# Publish KernelQ images to ECR with an immutable Git SHA tag (Day 126).
#
# Required: AWS_REGION, AWS_ACCOUNT_ID
# Optional: IMAGE_TAG (default: git rev-parse HEAD), DRY_RUN=true, CREATE_ECR_REPOS=true
#
# Run from repository root.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

fail() {
  echo "FAIL: $*" >&2
  if [[ "${DRY_RUN:-}" == "true" ]]; then
    echo "event=ecr_publish_dry_run success=false" >&2
  else
    echo "event=ecr_publish success=false" >&2
  fi
  exit 1
}

DRY_RUN="${DRY_RUN:-false}"
AWS_REGION="${AWS_REGION:-}"
AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID:-}"
IMAGE_TAG="${IMAGE_TAG:-}"
CREATE_ECR_REPOS="${CREATE_ECR_REPOS:-false}"

[[ -n "${AWS_REGION}" ]] || fail "AWS_REGION is required"
[[ -n "${AWS_ACCOUNT_ID}" ]] || fail "AWS_ACCOUNT_ID is required"
[[ "${AWS_ACCOUNT_ID}" =~ ^[0-9]{12}$ ]] || fail "AWS_ACCOUNT_ID must be a 12-digit account id"

command -v git >/dev/null 2>&1 || fail "git is required"
command -v docker >/dev/null 2>&1 || fail "docker is required"

if [[ -z "${IMAGE_TAG}" ]]; then
  IMAGE_TAG="$(git rev-parse HEAD)"
fi
[[ "${IMAGE_TAG}" =~ ^[0-9a-fA-F]{7,64}$ ]] || fail "IMAGE_TAG looks malformed: ${IMAGE_TAG}"
[[ "${IMAGE_TAG}" != "latest" ]] || fail "refusing to publish tag 'latest'"

WORKER_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/kernelq-worker:${IMAGE_TAG}"
CONTROL_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/kernelq-control-plane:${IMAGE_TAG}"

echo "==> ECR publish plan"
echo "AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID}"
echo "AWS_REGION=${AWS_REGION}"
echo "IMAGE_TAG=${IMAGE_TAG}"
echo "WORKER_URI=${WORKER_URI}"
echo "CONTROL_URI=${CONTROL_URI}"
echo "DRY_RUN=${DRY_RUN}"
echo "CREATE_ECR_REPOS=${CREATE_ECR_REPOS}"

if [[ "${DRY_RUN}" == "true" ]]; then
  echo "==> Dry-run commands (not executed)"
  echo "aws sts get-caller-identity --query Account --output text"
  echo "aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
  echo "docker build -f deploy/docker/Dockerfile.worker -t ${WORKER_URI} ."
  echo "docker build -f deploy/docker/Dockerfile.control-plane -t ${CONTROL_URI} ."
  echo "docker push ${WORKER_URI}"
  echo "docker push ${CONTROL_URI}"
  echo "event=ecr_publish_dry_run success=true"
  exit 0
fi

command -v aws >/dev/null 2>&1 || fail "aws CLI is required for real publish"

RESOLVED_ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>/dev/null || true)"
[[ -n "${RESOLVED_ACCOUNT}" ]] || fail "unable to resolve AWS account via sts"
[[ "${RESOLVED_ACCOUNT}" == "${AWS_ACCOUNT_ID}" ]] \
  || fail "AWS account mismatch: expected ${AWS_ACCOUNT_ID}, got ${RESOLVED_ACCOUNT}"

ensure_repo() {
  local name="$1"
  if aws ecr describe-repositories --repository-names "${name}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "ECR repository exists: ${name}"
    return 0
  fi
  if [[ "${CREATE_ECR_REPOS}" != "true" ]]; then
    fail "ECR repository ${name} missing (set CREATE_ECR_REPOS=true to create)"
  fi
  echo "Creating ECR repository ${name} (immutable tags, scan on push)..."
  aws ecr create-repository \
    --repository-name "${name}" \
    --region "${AWS_REGION}" \
    --image-tag-mutability IMMUTABLE \
    --image-scanning-configuration scanOnPush=true \
    >/dev/null
}

ensure_repo "kernelq-worker"
ensure_repo "kernelq-control-plane"

echo "==> Authenticating Docker to ECR (token not printed)..."
aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com" \
  >/dev/null

echo "==> Building images..."
docker build -f deploy/docker/Dockerfile.worker -t "${WORKER_URI}" .
docker build -f deploy/docker/Dockerfile.control-plane -t "${CONTROL_URI}" .

echo "==> Pushing images..."
docker push "${WORKER_URI}"
docker push "${CONTROL_URI}"

echo "WORKER_URI=${WORKER_URI}"
echo "CONTROL_URI=${CONTROL_URI}"
echo "event=ecr_publish success=true"
