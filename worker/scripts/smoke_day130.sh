#!/usr/bin/env bash
#
# Day 130 — final production-readiness regression orchestrator.
# Distinguishes required checks from optional environment-dependent checks.
#
# Run from repository root:
#   ./worker/scripts/smoke_day130.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker ]] || [[ ! -d control_plane ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

ROOT_DIR="$(pwd)"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-day130-$$"
DIAG_DIR="${TMP_DIR}/diagnostics"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"

REQUIRED_PASSED=0
REQUIRED_FAILED=0
OPTIONAL_PASSED=0
OPTIONAL_SKIPPED=0

fail_required() {
  echo "FAIL: $*" >&2
  echo "event=smoke_day130 success=false" >&2
  exit 1
}

cleanup() {
  # Best-effort: stop any day130-started background processes tagged in PID file.
  if [[ -f "${TMP_DIR}/pids.txt" ]]; then
    while read -r pid; do
      [[ -n "${pid}" ]] || continue
      kill -INT "${pid}" 2>/dev/null || true
      wait "${pid}" 2>/dev/null || true
    done <"${TMP_DIR}/pids.txt" || true
  fi
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v go >/dev/null 2>&1 || fail_required "go is required"
command -v python3 >/dev/null 2>&1 || fail_required "python3 is required"
command -v kubectl >/dev/null 2>&1 || fail_required "kubectl is required"
command -v git >/dev/null 2>&1 || fail_required "git is required"

mkdir -p "${TMP_DIR}" "${DIAG_DIR}"
git status --short >"${STATUS_BEFORE}"

run_required() {
  local name="$1"
  shift
  echo ""
  echo "======== REQUIRED: ${name} ========"
  if "$@" >"${DIAG_DIR}/${name}.log" 2>&1; then
    echo "PASS: ${name}"
    REQUIRED_PASSED=$((REQUIRED_PASSED + 1))
    grep -E 'event=|PASS:|ok  ' "${DIAG_DIR}/${name}.log" | tail -n 5 || true
  else
    echo "FAIL: ${name}" >&2
    echo "--- diagnostics (${name}) ---" >&2
    tail -n 80 "${DIAG_DIR}/${name}.log" >&2 || true
    REQUIRED_FAILED=$((REQUIRED_FAILED + 1))
    fail_required "${name} failed"
  fi
}

run_optional() {
  local name="$1"
  shift
  echo ""
  echo "======== OPTIONAL: ${name} ========"
  if "$@" >"${DIAG_DIR}/${name}.log" 2>&1; then
    if grep -qE '^SKIP:' "${DIAG_DIR}/${name}.log"; then
      echo "SKIP: ${name}"
      OPTIONAL_SKIPPED=$((OPTIONAL_SKIPPED + 1))
    else
      echo "PASS: ${name}"
      OPTIONAL_PASSED=$((OPTIONAL_PASSED + 1))
    fi
    grep -E 'event=|PASS:|SKIP:' "${DIAG_DIR}/${name}.log" | tail -n 8 || true
  else
    # Optional checks that hard-fail when infra missing should prefer SKIP.
    # Treat non-zero as failure of the Day 130 gate only if not a documented skip.
    if grep -qE '^SKIP:' "${DIAG_DIR}/${name}.log"; then
      echo "SKIP: ${name}"
      OPTIONAL_SKIPPED=$((OPTIONAL_SKIPPED + 1))
      return 0
    fi
    echo "FAIL: optional ${name}" >&2
    tail -n 60 "${DIAG_DIR}/${name}.log" >&2 || true
    fail_required "optional check ${name} failed unexpectedly"
  fi
}

docker_available() {
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1
}

echo "==> Day 130 production-readiness verification"
echo "root=${ROOT_DIR}"

# --- Required: unit/integration tests ---
run_required "go_tests" bash -c "cd '${ROOT_DIR}/worker' && go test ./... -count=1"
run_required "python_tests" bash -c "cd '${ROOT_DIR}' && PYTHONPATH=. python3 -m pytest control_plane/tests -q"

# --- Required: kustomize renders ---
run_required "kustomize_base" kubectl kustomize deploy/kubernetes/base
run_required "kustomize_local" kubectl kustomize deploy/kubernetes/overlays/local
run_required "kustomize_production" kubectl kustomize deploy/kubernetes/overlays/production
run_required "kustomize_eks" kubectl kustomize deploy/kubernetes/overlays/eks
run_required "kustomize_eks_observability" kubectl kustomize deploy/kubernetes/overlays/eks-observability
run_required "kustomize_fluent_bit" kubectl kustomize deploy/observability/fluent-bit
run_required "kustomize_prometheus" kubectl kustomize deploy/observability/prometheus

# --- Required: offline / deterministic smokes ---
run_required "smoke_k8s_policies" ./worker/scripts/smoke_k8s_policies.sh
run_required "smoke_eks_config" ./worker/scripts/smoke_eks_config.sh
run_required "smoke_logging" ./worker/scripts/smoke_logging.sh
run_required "smoke_cloudwatch_config" ./worker/scripts/smoke_cloudwatch_config.sh
run_required "smoke_monitoring" ./worker/scripts/smoke_monitoring.sh
run_required "smoke_dashboard" ./worker/scripts/smoke_dashboard.sh
run_required "smoke_grpc_trace" ./worker/scripts/smoke_grpc_trace.sh
run_required "smoke_resilience" ./worker/scripts/smoke_resilience.sh

# Core idempotency that needs Redis — required if docker+redis can start; else skip as optional.
if docker_available; then
  echo ""
  echo "======== Ensuring local Redis/Kafka for integration smokes ========"
  docker compose up -d redis >/dev/null 2>&1 || true
  # Wait briefly for redis
  for _ in $(seq 1 30); do
    if docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
      break
    fi
    sleep 0.2
  done
  if docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
    run_required "smoke_redis_idempotency" ./worker/scripts/smoke_redis_idempotency.sh
    run_required "smoke_worker_execution_idempotency" ./worker/scripts/smoke_worker_execution_idempotency.sh
  else
    run_optional "smoke_redis_idempotency" bash -c 'echo "SKIP: redis not ready"; exit 0'
    run_optional "smoke_worker_execution_idempotency" bash -c 'echo "SKIP: redis not ready"; exit 0'
  fi

  docker compose up -d zookeeper kafka >/dev/null 2>&1 || true
  for _ in $(seq 1 60); do
    if docker exec kernelq-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  if docker exec kernelq-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
    ./infra/kafka/create-topics.sh >/dev/null 2>&1 || true
    run_optional "smoke_kafka_execution_replay" ./worker/scripts/smoke_kafka_execution_replay.sh
    run_optional "smoke_kafka_trace" ./worker/scripts/smoke_kafka_trace.sh
  else
    run_optional "smoke_kafka_execution_replay" bash -c 'echo "SKIP: kafka not ready"; exit 0'
    run_optional "smoke_kafka_trace" bash -c 'echo "SKIP: kafka not ready"; exit 0'
  fi
else
  run_optional "smoke_redis_idempotency" bash -c 'echo "SKIP: docker unavailable"; exit 0'
  run_optional "smoke_worker_execution_idempotency" bash -c 'echo "SKIP: docker unavailable"; exit 0'
  run_optional "smoke_kafka_execution_replay" bash -c 'echo "SKIP: docker unavailable"; exit 0'
  run_optional "smoke_kafka_trace" bash -c 'echo "SKIP: docker unavailable"; exit 0'
fi

# Optional cluster-dependent
run_optional "smoke_k8s" ./worker/scripts/smoke_k8s_resilience.sh
# Note: smoke_k8s.sh is heavier; use resilience skip pattern. Also try smoke_k8s if cluster up.
if kubectl cluster-info >/dev/null 2>&1; then
  run_optional "smoke_k8s_local" ./worker/scripts/smoke_k8s.sh
else
  run_optional "smoke_k8s_local" bash -c 'echo "SKIP: no cluster reachable"; exit 0'
fi

# Tracked files unchanged
git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  echo "git status changed:" >&2
  diff -u "${STATUS_BEFORE}" "${STATUS_AFTER}" >&2 || true
  fail_required "tracked/working tree mutated during Day 130 smoke"
fi

# Leftover process hint (advisory; do not fail on unrelated names)
echo ""
echo "======== Process check (advisory) ========"
if pgrep -fl 'cmd/consumer|grpc-server|kernelq-consumer' >/dev/null 2>&1; then
  echo "WARN: possible leftover KernelQ-related processes:"
  pgrep -fl 'cmd/consumer|grpc-server|kernelq-consumer' || true
else
  echo "No obvious leftover consumer/grpc-server processes"
fi

echo ""
echo "required_checks_passed=${REQUIRED_PASSED}"
echo "required_checks_failed=${REQUIRED_FAILED}"
echo "optional_checks_passed=${OPTIONAL_PASSED}"
echo "optional_checks_skipped=${OPTIONAL_SKIPPED}"

if [[ "${REQUIRED_FAILED}" -ne 0 ]]; then
  fail_required "required checks failed"
fi

echo "PASS: Day 130 production-readiness verification succeeded"
echo "event=smoke_day130 success=true"
