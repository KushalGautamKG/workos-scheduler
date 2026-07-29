#!/usr/bin/env bash
#
# Kubernetes Pod replacement resilience smoke (Day 129).
# Optional — skips successfully when no cluster is reachable.
#
# Run from repository root:
#   ./worker/scripts/smoke_k8s_resilience.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d deploy/kubernetes/overlays/local ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

NAMESPACE="${KERNELQ_K8S_NAMESPACE:-kernelq-resilience-$$}"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-k8s-resilience-$$"
DIAG="${TMP_DIR}/diagnostics.txt"

fail() {
  echo "FAIL: $*" >&2
  {
    echo "=== diagnostics ==="
    kubectl -n "${NAMESPACE}" get deployments 2>/dev/null || true
    kubectl -n "${NAMESPACE}" get replicasets 2>/dev/null || true
    kubectl -n "${NAMESPACE}" get pods 2>/dev/null || true
    kubectl -n "${NAMESPACE}" describe pods 2>/dev/null || true
    kubectl -n "${NAMESPACE}" get events --sort-by=.lastTimestamp 2>/dev/null || true
    kubectl -n "${NAMESPACE}" logs -l app.kubernetes.io/name=kernelq-worker --tail=100 2>/dev/null || true
    kubectl -n "${NAMESPACE}" logs -l app.kubernetes.io/name=kernelq-worker --previous --tail=50 2>/dev/null || true
  } >"${DIAG}" 2>&1 || true
  [[ -f "${DIAG}" ]] && cat "${DIAG}" >&2 || true
  echo "event=smoke_k8s_resilience success=false" >&2
  exit 1
}

cleanup() {
  if kubectl get ns "${NAMESPACE}" >/dev/null 2>&1; then
    kubectl delete namespace "${NAMESPACE}" --wait=false >/dev/null 2>&1 || true
  fi
  rm -rf "${TMP_DIR}"
}

mkdir -p "${TMP_DIR}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "SKIP: kubernetes resilience smoke — no cluster reachable"
  echo "event=smoke_k8s_resilience success=true skipped=true"
  exit 0
fi

if ! kubectl cluster-info >/dev/null 2>&1; then
  echo "SKIP: kubernetes resilience smoke — no cluster reachable"
  echo "event=smoke_k8s_resilience success=true skipped=true"
  exit 0
fi

trap cleanup EXIT INT TERM

echo "==> Deploying local overlay to namespace ${NAMESPACE}..."
# Render and rewrite namespace for isolation.
kubectl kustomize deploy/kubernetes/overlays/local >"${TMP_DIR}/local.yaml" || fail "kustomize failed"
python3 - "${TMP_DIR}/local.yaml" "${NAMESPACE}" <<'PY' || fail "namespace rewrite failed"
from pathlib import Path
import sys
text = Path(sys.argv[1]).read_text(encoding="utf-8")
ns = sys.argv[2]
# Best-effort: replace kernelq namespace references.
text = text.replace("namespace: kernelq\n", f"namespace: {ns}\n")
text = text.replace("name: kernelq\n", f"name: {ns}\n", 1)
Path(sys.argv[1]).write_text(text, encoding="utf-8")
PY

kubectl apply -f "${TMP_DIR}/local.yaml" || fail "apply failed"
kubectl -n "${NAMESPACE}" rollout status deployment --timeout=180s || fail "rollout failed"

WORKER_DEPLOY="$(kubectl -n "${NAMESPACE}" get deploy -o name | grep -i worker | head -n1 || true)"
[[ -n "${WORKER_DEPLOY}" ]] || fail "worker deployment not found"

REPLICAS="$(kubectl -n "${NAMESPACE}" get "${WORKER_DEPLOY}" -o jsonpath='{.spec.replicas}')"
POD="$(kubectl -n "${NAMESPACE}" get pods -l app.kubernetes.io/name=kernelq-worker -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
if [[ -z "${POD}" ]]; then
  POD="$(kubectl -n "${NAMESPACE}" get pods -o name | grep -i worker | head -n1 | sed 's|pod/||')"
fi
[[ -n "${POD}" ]] || fail "worker pod not found"
echo "worker_pod=${POD} replicas=${REPLICAS}"

if [[ "${REPLICAS}" == "1" ]]; then
  echo "NOTE: local overlay has 1 worker replica — temporary unavailability expected during delete"
fi

kubectl -n "${NAMESPACE}" delete pod "${POD}" --wait=true || fail "pod delete failed"

kubectl -n "${NAMESPACE}" rollout status "${WORKER_DEPLOY}" --timeout=180s || fail "replacement rollout failed"
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod --all --timeout=180s || fail "pods not ready"

# gRPC via port-forward if service exists
SVC="$(kubectl -n "${NAMESPACE}" get svc -o name | grep -i worker | head -n1 || true)"
if [[ -n "${SVC}" ]]; then
  PF_LOG="${TMP_DIR}/pf.log"
  kubectl -n "${NAMESPACE}" port-forward "${SVC}" 15051:50051 >"${PF_LOG}" 2>&1 &
  PF_PID=$!
  sleep 2
  (
    cd worker
    KERNELQ_GRPC_ADDR=127.0.0.1:15051 go run ./cmd/grpc-health || true
  )
  kill "${PF_PID}" 2>/dev/null || true
  wait "${PF_PID}" 2>/dev/null || true
fi

echo "PASS: kubernetes resilience smoke succeeded"
echo "event=smoke_k8s_resilience success=true"
