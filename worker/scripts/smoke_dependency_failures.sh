#!/usr/bin/env bash
#
# Dependency failure smoke: Redis, Kafka, gRPC (Day 129).
# Local docker-compose only — no cloud services.
#
# Run from repository root:
#   ./worker/scripts/smoke_dependency_failures.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

ROOT_DIR="$(pwd)"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-dep-fail-$$"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"
LOG="${TMP_DIR}/scenarios.log"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_dependency_failures success=false" >&2
  [[ -f "${LOG}" ]] && cat "${LOG}" >&2 || true
  exit 1
}

cleanup() {
  # Best-effort restore infra if we stopped it.
  docker compose start redis >/dev/null 2>&1 || true
  docker compose start kafka >/dev/null 2>&1 || true
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v go >/dev/null 2>&1 || fail "go required"
command -v git >/dev/null 2>&1 || fail "git required"
mkdir -p "${TMP_DIR}"
git status --short >"${STATUS_BEFORE}"

echo "==> In-process dependency classification scenarios..."
(
  cd "${ROOT_DIR}/worker"
  go run ./cmd/resilience-scenarios
) >"${LOG}" 2>&1 || fail "resilience-scenarios failed"

grep -Fq 'event=scenario_redis_unavailable success=true' "${LOG}" || fail "redis scenario missing"
grep -Fq 'event=scenario_grpc_unavailable success=true' "${LOG}" || fail "grpc scenario missing"
grep -Fq 'event=scenario_result_publish_failure success=true' "${LOG}" || fail "publish scenario missing"

if docker compose ps --status running --services 2>/dev/null | grep -qx redis; then
  echo "==> Live Redis stop/restore..."
  docker exec kernelq-redis redis-cli ping | grep -q PONG || fail "redis not healthy before outage"
  docker compose stop redis
  sleep 1
  if docker exec kernelq-redis redis-cli ping >/dev/null 2>&1; then
    fail "redis still responding after stop"
  fi
  echo "redis_outage_detected=true"
  docker compose start redis
  for _ in $(seq 1 30); do
    if docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
      break
    fi
    sleep 0.3
  done
  docker exec kernelq-redis redis-cli ping | grep -q PONG || fail "redis did not recover"
  echo "redis_restored=true"
else
  echo "SKIP: redis container not running for live outage"
fi

if docker compose ps --status running --services 2>/dev/null | grep -qx kafka; then
  echo "==> Live Kafka stop/restore (bounded)..."
  START_MS="$(python3 -c 'import time; print(int(time.time()*1000))')"
  docker compose stop kafka
  sleep 1
  # Publish attempt should fail while broker is down (from host).
  if docker exec kernelq-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
    echo "WARN: kafka still reachable after stop (timing); continuing"
  else
    echo "kafka_outage_detected=true"
  fi
  docker compose start kafka zookeeper >/dev/null 2>&1 || docker compose start kafka
  for _ in $(seq 1 60); do
    if docker exec kernelq-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  docker exec kernelq-kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1 \
    || fail "kafka did not recover"
  END_MS="$(python3 -c 'import time; print(int(time.time()*1000))')"
  echo "kafka_restored=true observed_recovery_ms=$((END_MS - START_MS))"
else
  echo "SKIP: kafka container not running for live outage"
fi

echo "==> gRPC unreachable target (deadline-bounded)..."
(
  cd worker
  go test ./internal/grpc -run 'TestClientServerUnavailable|TestClientTimeoutPropagation' -count=1
) || fail "grpc unavailable/timeout tests failed"

git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  fail "tracked files mutated"
fi

echo "PASS: dependency failure smoke succeeded"
echo "event=smoke_dependency_failures success=true"
