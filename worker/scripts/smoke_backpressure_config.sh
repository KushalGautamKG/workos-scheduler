#!/usr/bin/env bash
#
# Smoke test: cmd/consumer prints backpressure env config at startup.
#
# Run from the repository root:
#   ./worker/scripts/smoke_backpressure_config.sh
#
# Requires: Docker, Go, and Kafka on localhost:9092.

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/consumer ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

CONSUMER_BIN="${TMPDIR:-/tmp}/kernelq-consumer-smoke"
DEFAULT_LOG="${TMPDIR:-/tmp}/kernelq-backpressure-default.log"
ENABLED_LOG="${TMPDIR:-/tmp}/kernelq-backpressure-enabled.log"

run_consumer_briefly() {
  local log_file="$1"
  shift

  : >"${log_file}"
  env "$@" "${CONSUMER_BIN}" >"${log_file}" 2>&1 &
  local pid=$!
  sleep 2
  kill -INT "${pid}" 2>/dev/null || true
  wait "${pid}" 2>/dev/null || true
}

assert_grep() {
  local log_file="$1"
  local pattern="$2"
  local label="$3"

  if ! grep -Fq "${pattern}" "${log_file}"; then
    echo "FAIL: expected ${label} in ${log_file}" >&2
    cat "${log_file}" >&2
    exit 1
  fi
}

echo "==> Starting Kafka and Zookeeper..."
docker compose up -d kafka zookeeper

echo "==> Building consumer..."
(
  cd worker
  go build -o "${CONSUMER_BIN}" ./cmd/consumer
)

echo "==> Checking default backpressure config..."
run_consumer_briefly "${DEFAULT_LOG}"
assert_grep "${DEFAULT_LOG}" "backpressure_enabled=false" "backpressure_enabled=false"

echo "==> Checking enabled backpressure config..."
run_consumer_briefly "${ENABLED_LOG}" \
  KERNELQ_WORKER_BACKPRESSURE_ENABLED=true \
  KERNELQ_WORKER_BACKPRESSURE_HIGH_RATIO=0.8 \
  KERNELQ_WORKER_BACKPRESSURE_LOW_RATIO=0.5
assert_grep "${ENABLED_LOG}" "backpressure_enabled=true" "backpressure_enabled=true"
assert_grep "${ENABLED_LOG}" "backpressure_high_ratio=0.8" "backpressure_high_ratio=0.8"
assert_grep "${ENABLED_LOG}" "backpressure_low_ratio=0.5" "backpressure_low_ratio=0.5"

echo "PASS: worker backpressure config smoke test succeeded"
echo "event=smoke_worker_backpressure_config success=true"
