#!/usr/bin/env bash
#
# Smoke test: dispatch message on kernelq.jobs.dispatch → worker executes →
# result appears on kernelq.jobs.results.
#
# Run from the repository root:
#   ./worker/scripts/smoke_worker_result.sh
#
# Requires: Docker, docker compose, Go, and network access to localhost:9092.

set -euo pipefail

# --- Sanity check: this script expects to run from the repo root ---
if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/consumer ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

KAFKA_CONTAINER="kernelq-kafka"
BOOTSTRAP_SERVER="kafka:29092"
DISPATCH_TOPIC="kernelq.jobs.dispatch"
RESULTS_TOPIC="kernelq.jobs.results"
WORKER_LOG="/tmp/kernelq-worker-smoke.log"
WORKER_PID=""

# Unique job id so we can find our result among older Kafka messages.
JOB_ID="day47-smoke-$(date +%s)"

# How long to wait for the result before giving up (seconds).
RESULT_WAIT_SECONDS=30

cleanup() {
  # Stop the worker gracefully (same signal as Ctrl+C).
  if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
    echo "Stopping worker (PID ${WORKER_PID})..."
    kill -INT "${WORKER_PID}" || true
    wait "${WORKER_PID}" 2>/dev/null || true
  fi

  echo ""
  echo "=== Worker logs (${WORKER_LOG}) ==="
  if [[ -f "${WORKER_LOG}" ]]; then
    cat "${WORKER_LOG}"
  else
    echo "(no log file)"
  fi
}

trap cleanup EXIT

echo "==> Starting Kafka and Zookeeper..."
docker compose up -d kafka zookeeper

echo "==> Creating Kafka topics..."
./infra/kafka/create-topics.sh

echo "==> Building worker binary..."
(
  cd worker
  go build -o consumer ./cmd/consumer
)

echo "==> Starting worker in background..."
./worker/consumer > "${WORKER_LOG}" 2>&1 &
WORKER_PID=$!
echo "Worker PID: ${WORKER_PID}"

# Give the consumer time to connect and subscribe.
echo "==> Waiting for worker to start..."
sleep 5

# Valid DispatchEvent JSON (matches worker/internal/worker/dispatch_event.go).
DISPATCH_JSON=$(
  cat <<EOF
{"event_type":"job.dispatch","job_id":"${JOB_ID}","tenant_id":"tenant-a","priority":999999,"state":"dispatched","payload":{"kind":"day47-smoke"}}
EOF
)

echo "==> Producing dispatch message (job_id=${JOB_ID})..."
echo "${DISPATCH_JSON}" | docker exec -i "${KAFKA_CONTAINER}" kafka-console-producer \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${DISPATCH_TOPIC}"

echo "==> Waiting for result on ${RESULTS_TOPIC} (timeout ${RESULT_WAIT_SECONDS}s)..."
FOUND=0
SECONDS=0
while [[ "${SECONDS}" -lt "${RESULT_WAIT_SECONDS}" ]]; do
  # Read recent topic history and look for our job_id.
  # --timeout-ms keeps the consumer from blocking forever if the topic is quiet.
  if docker exec "${KAFKA_CONTAINER}" kafka-console-consumer \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${RESULTS_TOPIC}" \
    --from-beginning \
    --timeout-ms 3000 \
    --max-messages 500 2>/dev/null | grep -Fq "\"job_id\":\"${JOB_ID}\""; then
    FOUND=1
    break
  fi
  sleep 2
  SECONDS=$((SECONDS + 2))
done

if [[ "${FOUND}" -eq 1 ]]; then
  echo "PASS: Found result for job_id=${JOB_ID} on ${RESULTS_TOPIC}"
  exit 0
else
  echo "FAIL: No result for job_id=${JOB_ID} on ${RESULTS_TOPIC} within ${RESULT_WAIT_SECONDS}s" >&2
  exit 1
fi
