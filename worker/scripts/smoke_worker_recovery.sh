#!/usr/bin/env bash
#
# Worker crash / fault recovery smoke (Day 129).
# Uses in-process fault injection + optional Kafka republish path.
#
# Run from repository root:
#   ./worker/scripts/smoke_worker_recovery.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/internal/faults ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

ROOT_DIR="$(pwd)"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-worker-recovery-$$"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"
SCENARIO_LOG="${TMP_DIR}/scenarios.log"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_worker_recovery success=false" >&2
  [[ -f "${SCENARIO_LOG}" ]] && cat "${SCENARIO_LOG}" >&2 || true
  exit 1
}

cleanup() {
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v go >/dev/null 2>&1 || fail "go is required"
command -v git >/dev/null 2>&1 || fail "git is required"
mkdir -p "${TMP_DIR}"
git status --short >"${STATUS_BEFORE}"

echo "==> Fault injector unit tests..."
(
  cd "${ROOT_DIR}/worker"
  go test ./internal/faults ./internal/metrics -count=1
) || fail "fault/metrics unit tests failed"

echo "==> Deterministic fault recovery scenario (before_claim → retry)..."
(
  cd "${ROOT_DIR}/worker"
  go run ./cmd/resilience-scenarios
) >"${SCENARIO_LOG}" 2>&1 || fail "resilience-scenarios failed"

grep -Fq 'event=scenario_fault_before_claim_recovery success=true' "${SCENARIO_LOG}" \
  || fail "fault recovery scenario missing"
grep -Fq 'event=scenario_duplicate_delivery success=true' "${SCENARIO_LOG}" \
  || fail "duplicate delivery scenario missing"
grep -Fq 'event=scenario_result_publish_failure success=true' "${SCENARIO_LOG}" \
  || fail "result publish failure scenario missing"
grep -Fq 'event=resilience_scenarios success=true' "${SCENARIO_LOG}" \
  || fail "scenarios overall failure"

# Optional Kafka path: publish once with faulting worker, republish with healthy worker.
KAFKA_CONTAINER="kernelq-kafka"
if docker compose ps --status running --services 2>/dev/null | grep -qx kafka \
  && docker compose ps --status running --services 2>/dev/null | grep -qx redis; then
  echo "==> Kafka+Redis recovery path (optional local infra)..."
  RUN_ID="$(date +%s)"
  JOB_ID="day129-recovery-${RUN_ID}"
  GROUP_ID="kernelq-smoke-recovery-${RUN_ID}"
  CONSUMER_BIN="${TMP_DIR}/consumer"
  WORKER_LOG="${TMP_DIR}/worker.log"
  WORKER_PID=""

  stop_worker() {
    if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
      kill -INT "${WORKER_PID}" 2>/dev/null || true
      wait "${WORKER_PID}" 2>/dev/null || true
      WORKER_PID=""
    fi
  }

  (
    cd worker
    go build -o "${CONSUMER_BIN}" ./cmd/consumer
  ) || fail "build consumer failed"

  docker exec kernelq-redis redis-cli DEL "kernelq:idempotency:execution:${JOB_ID}:0" >/dev/null || true

  : >"${WORKER_LOG}"
  KERNELQ_ENVIRONMENT=local \
  KERNELQ_FAULTS_ENABLED=true \
  KERNELQ_FAULT_POINT=before_claim \
  KERNELQ_FAULT_MODE=error \
  KERNELQ_FAULT_COUNT=1 \
  KERNELQ_WORKER_IDEMPOTENCY_BACKEND=redis \
  KERNELQ_WORKER_COUNT=1 \
  KERNELQ_KAFKA_GROUP_ID="${GROUP_ID}" \
  KERNELQ_KAFKA_AUTO_OFFSET_RESET=latest \
  KERNELQ_LOG_FORMAT=json \
    "${CONSUMER_BIN}" >"${WORKER_LOG}" 2>&1 &
  WORKER_PID=$!
  sleep 2

  PAYLOAD="$(printf '{"event_type":"job.dispatch","job_id":"%s","tenant_id":"tenant-a","priority":1,"state":"dispatched","attempt":0,"payload":{"kind":"day129-recovery"}}' "${JOB_ID}")"
  echo "${PAYLOAD}" | docker exec -i "${KAFKA_CONTAINER}" \
    kafka-console-producer --bootstrap-server kafka:29092 --topic kernelq.jobs.dispatch >/dev/null

  for _ in $(seq 1 40); do
    if grep -q 'test fault injected' "${WORKER_LOG}" 2>/dev/null; then
      break
    fi
    sleep 0.25
  done
  grep -q 'test fault injected' "${WORKER_LOG}" || fail "fault not observed in worker log"
  stop_worker

  # Republish same logical job (handler error path may DLQ; recovery = redelivery/republish).
  : >"${WORKER_LOG}"
  KERNELQ_ENVIRONMENT=local \
  KERNELQ_FAULTS_ENABLED=false \
  KERNELQ_WORKER_IDEMPOTENCY_BACKEND=redis \
  KERNELQ_WORKER_COUNT=1 \
  KERNELQ_KAFKA_GROUP_ID="${GROUP_ID}-2" \
  KERNELQ_KAFKA_AUTO_OFFSET_RESET=latest \
  KERNELQ_LOG_FORMAT=json \
    "${CONSUMER_BIN}" >"${WORKER_LOG}" 2>&1 &
  WORKER_PID=$!
  sleep 2
  echo "${PAYLOAD}" | docker exec -i "${KAFKA_CONTAINER}" \
    kafka-console-producer --bootstrap-server kafka:29092 --topic kernelq.jobs.dispatch >/dev/null

  for _ in $(seq 1 60); do
    if grep -q 'job execution completed' "${WORKER_LOG}" 2>/dev/null; then
      break
    fi
    sleep 0.25
  done
  grep -q 'job execution completed' "${WORKER_LOG}" || fail "recovery execution not completed"
  stop_worker
  echo "kafka_recovery_path=ok"
else
  echo "SKIP: kafka/redis not running — in-process recovery still validated"
fi

git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  fail "tracked files mutated"
fi

echo "PASS: worker recovery smoke succeeded"
echo "event=smoke_worker_recovery success=true"
