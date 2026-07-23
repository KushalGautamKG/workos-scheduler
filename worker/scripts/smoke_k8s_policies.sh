#!/usr/bin/env bash
#
# Smoke test: render and assert Kubernetes production policies (Day 125).
# Does not require a live production cluster — kustomize + client dry-run only.
#
# Run from the repository root:
#   ./worker/scripts/smoke_k8s_policies.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d deploy/kubernetes/base ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

TMP_DIR="${TMPDIR:-/tmp}/kernelq-k8s-policies-$$"
BASE_OUT="${TMP_DIR}/base.yaml"
LOCAL_OUT="${TMP_DIR}/local.yaml"
PROD_OUT="${TMP_DIR}/production.yaml"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_k8s_policies success=false" >&2
  exit 1
}

cleanup() {
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"
mkdir -p "${TMP_DIR}"

echo "==> Rendering base / local / production overlays..."
kubectl kustomize deploy/kubernetes/base >"${BASE_OUT}" \
  || fail "kustomize base failed"
kubectl kustomize deploy/kubernetes/overlays/local >"${LOCAL_OUT}" \
  || fail "kustomize local failed"
kubectl kustomize deploy/kubernetes/overlays/production >"${PROD_OUT}" \
  || fail "kustomize production failed"

echo "==> Client-side validation..."
if kubectl cluster-info >/dev/null 2>&1; then
  kubectl apply --dry-run=client --validate=false -f "${LOCAL_OUT}" >/dev/null \
    || fail "dry-run local failed"
  kubectl apply --dry-run=client --validate=false -f "${PROD_OUT}" >/dev/null \
    || fail "dry-run production failed"
else
  # Newer kubectl still contacts the API server for multi-doc apply recognition.
  # Without a cluster, kustomize render + structural asserts below are the offline path.
  echo "    (no cluster reachable — skipping apply --dry-run; asserting rendered YAML)"
  python3 - "${LOCAL_OUT}" "${PROD_OUT}" <<'PY' || fail "rendered YAML failed to parse"
import sys
from pathlib import Path

def check(path: str) -> None:
    text = Path(path).read_text(encoding="utf-8")
    docs = [d for d in text.split("\n---\n") if d.strip()]
    if len(docs) < 5:
        raise SystemExit(f"{path}: expected multiple documents, got {len(docs)}")
    for doc in docs:
        if "kind:" not in doc:
            raise SystemExit(f"{path}: document missing kind")

check(sys.argv[1])
check(sys.argv[2])
print("parsed_ok=true")
PY
fi

assert_grep() {
  local file="$1"
  local pattern="$2"
  local msg="$3"
  grep -Eq -- "${pattern}" "${file}" || fail "${msg}"
}

echo "==> Asserting production policies..."

# Two replicas for each Deployment (YAML may list replicas: 2 twice).
REPLICA_LINES="$(grep -cE '^[[:space:]]*replicas:[[:space:]]*2[[:space:]]*$' "${PROD_OUT}" || true)"
[[ "${REPLICA_LINES}" -ge 2 ]] || fail "expected replicas: 2 for worker and control-plane (got ${REPLICA_LINES} matches)"

assert_grep "${PROD_OUT}" 'cpu:[[:space:]]*100m' "missing CPU requests (100m)"
assert_grep "${PROD_OUT}" 'memory:[[:space:]]*128Mi' "missing memory requests (128Mi)"
assert_grep "${PROD_OUT}" 'cpu:[[:space:]]*500m' "missing CPU limits (500m)"
assert_grep "${PROD_OUT}" 'memory:[[:space:]]*512Mi' "missing memory limits (512Mi)"

assert_grep "${PROD_OUT}" 'runAsNonRoot:[[:space:]]*true' "missing runAsNonRoot"
assert_grep "${PROD_OUT}" 'allowPrivilegeEscalation:[[:space:]]*false' "missing allowPrivilegeEscalation: false"
assert_grep "${PROD_OUT}" 'readOnlyRootFilesystem:[[:space:]]*true' "missing readOnlyRootFilesystem: true"
assert_grep "${PROD_OUT}" 'drop:' "missing capabilities drop"
assert_grep "${PROD_OUT}" '^[[:space:]]*-[[:space:]]+ALL[[:space:]]*$' "missing capabilities drop ALL"
assert_grep "${PROD_OUT}" 'seccompProfile:' "missing seccompProfile"
assert_grep "${PROD_OUT}" 'type:[[:space:]]*RuntimeDefault' "missing RuntimeDefault seccomp"

assert_grep "${PROD_OUT}" 'topologySpreadConstraints:' "missing topologySpreadConstraints"
assert_grep "${PROD_OUT}" 'whenUnsatisfiable:[[:space:]]*ScheduleAnyway' "missing ScheduleAnyway"
assert_grep "${PROD_OUT}" 'podAntiAffinity:' "missing preferred podAntiAffinity"
assert_grep "${PROD_OUT}" 'preferredDuringSchedulingIgnoredDuringExecution:' "missing preferred anti-affinity"

assert_grep "${PROD_OUT}" 'kind:[[:space:]]*PodDisruptionBudget' "missing PodDisruptionBudget"
assert_grep "${PROD_OUT}" 'minAvailable:[[:space:]]*1' "missing PDB minAvailable: 1"

assert_grep "${PROD_OUT}" 'type:[[:space:]]*RollingUpdate' "missing RollingUpdate strategy"
assert_grep "${PROD_OUT}" 'maxUnavailable:[[:space:]]*0' "missing maxUnavailable: 0"
assert_grep "${PROD_OUT}" 'maxSurge:[[:space:]]*1' "missing maxSurge: 1"
assert_grep "${PROD_OUT}" 'minReadySeconds:[[:space:]]*5' "missing minReadySeconds: 5"
assert_grep "${PROD_OUT}" 'revisionHistoryLimit:[[:space:]]*5' "missing revisionHistoryLimit: 5"
assert_grep "${PROD_OUT}" 'progressDeadlineSeconds:[[:space:]]*300' "missing progressDeadlineSeconds: 300"
assert_grep "${PROD_OUT}" 'terminationGracePeriodSeconds:[[:space:]]*30' "missing terminationGracePeriodSeconds: 30"

# Local overlay should stay single-replica (Day 124).
LOCAL_REPLICAS="$(grep -cE '^[[:space:]]*replicas:[[:space:]]*1[[:space:]]*$' "${LOCAL_OUT}" || true)"
[[ "${LOCAL_REPLICAS}" -ge 2 ]] || fail "local overlay should keep replicas: 1"

# Base should not include PDBs (production-only).
if grep -q 'kind: PodDisruptionBudget' "${BASE_OUT}"; then
  fail "base must not include PodDisruptionBudgets"
fi

echo "PASS: kubernetes policy smoke succeeded"
echo "event=smoke_k8s_policies success=true"
