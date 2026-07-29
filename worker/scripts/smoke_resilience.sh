#!/usr/bin/env bash
#
# Master resilience smoke (Day 129).
#
# Run from repository root:
#   ./worker/scripts/smoke_resilience.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

ROOT_DIR="$(pwd)"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-resilience-master-$$"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"
DIAG_DIR="${TMP_DIR}/diagnostics"
SKIPPED=0

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_resilience success=false" >&2
  exit 1
}

cleanup() {
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v git >/dev/null 2>&1 || fail "git required"
mkdir -p "${TMP_DIR}" "${DIAG_DIR}"
git status --short >"${STATUS_BEFORE}"

run_required() {
  local name="$1"
  shift
  echo "==> ${name}..."
  if ! "$@" >"${DIAG_DIR}/${name}.log" 2>&1; then
    echo "--- ${name} diagnostics ---" >&2
    cat "${DIAG_DIR}/${name}.log" >&2 || true
    fail "${name} failed"
  fi
  # Surface key event lines.
  grep -E 'event=|PASS:|SKIP:' "${DIAG_DIR}/${name}.log" || true
}

echo "==> Fault injector unit tests..."
(
  cd "${ROOT_DIR}/worker"
  go test ./internal/faults ./internal/metrics -count=1
) || fail "unit tests failed"

run_required "worker_recovery" "${ROOT_DIR}/worker/scripts/smoke_worker_recovery.sh"
run_required "dependency_failures" "${ROOT_DIR}/worker/scripts/smoke_dependency_failures.sh"

echo "==> Kubernetes resilience (optional)..."
set +e
"${ROOT_DIR}/worker/scripts/smoke_k8s_resilience.sh" >"${DIAG_DIR}/k8s_resilience.log" 2>&1
K8S_RC=$?
set -e
cat "${DIAG_DIR}/k8s_resilience.log"
if [[ "${K8S_RC}" -ne 0 ]]; then
  fail "k8s resilience failed"
fi
if grep -qE 'SKIP:.*k8s resilience' "${DIAG_DIR}/k8s_resilience.log"; then
  SKIPPED=$((SKIPPED + 1))
fi

git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  fail "tracked files mutated"
fi

echo "skipped_optional=${SKIPPED}"
echo "PASS: resilience smoke succeeded"
echo "event=smoke_resilience success=true"
