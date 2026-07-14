#!/usr/bin/env bash
#
# Smoke test: duplicate Kafka dispatch replay with Redis execution idempotency (Day 114).
# Same job_id + attempt + payload published twice → executor runs once.
#
# Run from the repository root:
#   ./worker/scripts/smoke_kafka_execution_replay.sh
#
# Requires: Docker Compose (redis, kafka, zookeeper), Go, localhost:9092 / :6379.

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/consumer ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

KAFKA_CONTAINER="kernelq-kafka"
BOOTSTRAP_SERVER="kafka:29092"
DISPATCH_TOPIC="kernelq.jobs.dispatch"
CONSUMER_BIN="${TMPDIR:-/tmp}/kernelq-consumer-kafka-replay"
WORKER_LOG="${TMPDIR:-/tmp}/kernelq-kafka-execution-replay.log"
WORKER_PID=""

RUN_ID="$(date +%s)"
JOB_ID="day114-replay-${RUN_ID}"
GROUP_ID="kernelq-smoke-replay-${RUN_ID}"
ATTEMPT=0
WAIT_SECONDS=30

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_kafka_execution_replay success=false" >&2
  if [[ -f "${WORKER_LOG}" ]]; then
    echo "" >&2
    echo "=== Worker logs (${WORKER_LOG}) ===" >&2
    cat "${WORKER_LOG}" >&2
  fi
  exit 1
}

cleanup() {
  if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
    echo "Stopping worker (PID ${WORKER_PID})..."
    kill -INT "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
    WORKER_PID=""
  fi
}

trap cleanup EXIT

stat_value() {
  local key="$1"
  local line
  line="$(grep -E "^${key}=" "${WORKER_LOG}" | tail -n1 || true)"
  if [[ -z "${line}" ]]; then
    echo ""
    return
  fi
  echo "${line#*=}"
}

assert_eq() {
  local name="$1"
  local want="$2"
  local got="$3"
  if [[ "${got}" != "${want}" ]]; then
    fail "expected ${name}=${want}, got ${got:-<missing>}"
  fi
  echo "${name}=${got}"
}

echo "==> Ensuring Redis is running..."
if ! docker compose ps --status running --services 2>/dev/null | grep -qx redis; then
  docker compose up -d redis
fi

for _ in $(seq 1 30); do
  if docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.2
done
if ! docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
  fail "redis not ready (kernelq-redis)"
fi

echo "==> Ensuring Kafka is running..."
if ! docker compose ps --status running --services 2>/dev/null | grep -qx kafka; then
  docker compose up -d zookeeper kafka
fi

for _ in $(seq 1 60); do
  if docker exec "${KAFKA_CONTAINER}" kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! docker exec "${KAFKA_CONTAINER}" kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list >/dev/null 2>&1; then
  fail "kafka not ready (${KAFKA_CONTAINER})"
fi

echo "==> Ensuring topics exist..."
./infra/kafka/create-topics.sh

# Clear any prior claim for this job_id/attempt (unique id makes this a no-op usually).
docker exec kernelq-redis redis-cli DEL \
  "kernelq:idempotency:execution:${JOB_ID}:${ATTEMPT}" >/dev/null

echo "==> Building worker binary..."
(
  cd worker
  go build -o "${CONSUMER_BIN}" ./cmd/consumer
)

: >"${WORKER_LOG}"
echo "==> Starting worker (Redis idempotency, unique group, auto.offset.reset=latest)..."
KERNELQ_WORKER_IDEMPOTENCY_BACKEND=redis \
KERNELQ_WORKER_COUNT=1 \
KERNELQ_KAFKA_GROUP_ID="${GROUP_ID}" \
KERNELQ_KAFKA_AUTO_OFFSET_RESET=latest \
"${CONSUMER_BIN}" >"${WORKER_LOG}" 2>&1 &
WORKER_PID=$!
echo "Worker PID: ${WORKER_PID} group_id=${GROUP_ID}"

# Wait for consumer assignment before producing (latest skips pre-start messages).
echo "==> Waiting for worker to subscribe..."
READY=0
for _ in $(seq 1 40); do
  if [[ -s "${WORKER_LOG}" ]] && grep -Eq "worker_idempotency_backend=redis|kafka_group_id=${GROUP_ID}" "${WORKER_LOG}"; then
    READY=1
    break
  fi
  if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
    fail "worker exited before becoming ready"
  fi
  sleep 0.25
done
if [[ "${READY}" -ne 1 ]]; then
  fail "worker did not print startup config"
fi
# Extra settle time for partition assignment after Subscribe.
sleep 5

DISPATCH_JSON=$(
  cat <<EOF
{"event_type":"job.dispatch","job_id":"${JOB_ID}","tenant_id":"tenant-a","priority":1,"state":"dispatched","attempt":${ATTEMPT},"payload":{"kind":"day114-kafka-replay"}}
EOF
)

echo "==> Producing identical dispatch event twice (job_id=${JOB_ID} attempt=${ATTEMPT})..."
echo "${DISPATCH_JSON}" | docker exec -i "${KAFKA_CONTAINER}" kafka-console-producer \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${DISPATCH_TOPIC}"
echo "${DISPATCH_JSON}" | docker exec -i "${KAFKA_CONTAINER}" kafka-console-producer \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${DISPATCH_TOPIC}"

echo "==> Waiting for both messages to process..."
SEEN_DUP=0
SECONDS=0
while [[ "${SECONDS}" -lt "${WAIT_SECONDS}" ]]; do
  if grep -Fq "event=duplicate_worker_execution job_id=${JOB_ID}" "${WORKER_LOG}" \
    && grep -Fq "received task job_id=${JOB_ID}" "${WORKER_LOG}"; then
    SEEN_DUP=1
    break
  fi
  if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
    fail "worker exited before processing completed"
  fi
  sleep 0.5
done
if [[ "${SEEN_DUP}" -ne 1 ]]; then
  fail "timed out waiting for execute + duplicate skip (job_id=${JOB_ID})"
fi
# Brief drain so both Handle completions are recorded before INT.
sleep 1

echo "==> Stopping worker cleanly..."
kill -INT "${WORKER_PID}" 2>/dev/null || true
wait "${WORKER_PID}" 2>/dev/null || true
WORKER_PID=""

# Allow shutdown stats to flush to the log file.
sleep 0.5

EXECUTOR_CALLS="$(stat_value executor_calls)"
DUPLICATE_EXECUTIONS="$(stat_value duplicate_executions)"
MESSAGES_PROCESSED="$(stat_value messages_processed)"
IDEMPOTENCY_ERRORS="$(stat_value idempotency_errors)"

assert_eq "executor_calls" "1" "${EXECUTOR_CALLS}"
assert_eq "duplicate_executions" "1" "${DUPLICATE_EXECUTIONS}"
# Shutdown prints messages_processed; smoke reports processed_messages for the Day 114 checklist.
assert_eq "messages_processed" "2" "${MESSAGES_PROCESSED}"
echo "processed_messages=${MESSAGES_PROCESSED}"
assert_eq "idempotency_errors" "0" "${IDEMPOTENCY_ERRORS}"

echo "PASS: kafka execution replay smoke test succeeded"
echo "event=smoke_kafka_execution_replay success=true"
