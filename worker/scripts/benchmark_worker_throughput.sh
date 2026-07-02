#!/usr/bin/env bash
#
# Benchmark Go worker throughput: produce N dispatch events, wait for worker completion.
#
# Run from the repository root:
#   ./worker/scripts/benchmark_worker_throughput.sh
#
# Environment (defaults):
#   COUNT=25
#   WORKERS=4
#   QUEUE_CAPACITY=100
#   WAIT_TIMEOUT_SECONDS=max(120, COUNT*3)
#
# Requires: Docker, Go, Kafka on localhost:9092.
#
# Completion is detected via worker stdout ("received task job_id=...") for this
# run's unique job prefix. Do not use kafka-console-consumer --from-beginning to
# count results — stale topic history causes missed events and false timeouts.

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/consumer ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

COUNT="${COUNT:-25}"
WORKERS="${WORKERS:-4}"
QUEUE_CAPACITY="${QUEUE_CAPACITY:-100}"
_default_wait=$((COUNT * 3))
if (( _default_wait < 120 )); then
  _default_wait=120
fi
WAIT_TIMEOUT_SECONDS="${WAIT_TIMEOUT_SECONDS:-${_default_wait}}"

KAFKA_CONTAINER="kernelq-kafka"
BOOTSTRAP_SERVER="kafka:29092"
DISPATCH_TOPIC="kernelq.jobs.dispatch"
CONSUMER_BIN="${TMPDIR:-/tmp}/kernelq-consumer-benchmark"
WORKER_LOG="${TMPDIR:-/tmp}/kernelq-worker-benchmark.log"
DISPATCH_FILE="${TMPDIR:-/tmp}/kernelq-worker-benchmark-dispatch.jsonl"
WORKER_PID=""

# Unique per run; job ids are deterministic within the run (00001..COUNT).
RUN_ID="$(date +%s)"
JOB_PREFIX="worker-bench-${RUN_ID}"

cleanup() {
  if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
    kill -INT "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
  fi
  rm -f "${DISPATCH_FILE}"
}

trap cleanup EXIT

# Count exact job ids for this run in worker stdout (executor completed + result published).
count_processed_jobs() {
  local processed=0
  local i
  local job_id

  for i in $(seq 1 "${COUNT}"); do
    job_id="$(printf "%s-%05d" "${JOB_PREFIX}" "${i}")"
    if grep -Fq "received task job_id=${job_id}" "${WORKER_LOG}"; then
      processed=$((processed + 1))
    fi
  done

  echo "${processed}"
}

echo "==> Starting Kafka and Zookeeper..."
docker compose up -d kafka zookeeper

echo "==> Creating Kafka topics..."
./infra/kafka/create-topics.sh

echo "==> Building consumer..."
(
  cd worker
  go build -o "${CONSUMER_BIN}" ./cmd/consumer
)

echo "==> Starting worker (workers=${WORKERS}, queue_capacity=${QUEUE_CAPACITY})..."
: >"${WORKER_LOG}"
KERNELQ_WORKER_COUNT="${WORKERS}" \
  KERNELQ_WORKER_QUEUE_CAPACITY="${QUEUE_CAPACITY}" \
  "${CONSUMER_BIN}" >"${WORKER_LOG}" 2>&1 &
WORKER_PID=$!

echo "==> Waiting for worker startup..."
for _ in $(seq 1 30); do
  if grep -Fq "KernelQ worker consumer started" "${WORKER_LOG}"; then
    break
  fi
  if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
    echo "FAIL: worker exited before startup" >&2
    cat "${WORKER_LOG}" >&2
    exit 1
  fi
  sleep 1
done

if ! grep -Fq "KernelQ worker consumer started" "${WORKER_LOG}"; then
  echo "FAIL: worker did not log startup within timeout" >&2
  cat "${WORKER_LOG}" >&2
  exit 1
fi

echo "==> Generating ${COUNT} dispatch events (prefix=${JOB_PREFIX})..."
: >"${DISPATCH_FILE}"
for i in $(seq 1 "${COUNT}"); do
  job_id="$(printf "%s-%05d" "${JOB_PREFIX}" "${i}")"
  printf '%s\n' \
    "{\"event_type\":\"job.dispatch\",\"job_id\":\"${job_id}\",\"tenant_id\":\"tenant-bench\",\"priority\":100,\"state\":\"dispatched\",\"payload\":{\"kind\":\"worker-throughput-bench\"}}" \
    >>"${DISPATCH_FILE}"
done

generated_jobs="${COUNT}"
bench_start_time="$(python3 -c 'import time; print(time.time())')"

echo "==> Producing dispatch batch..."
docker exec -i "${KAFKA_CONTAINER}" kafka-console-producer \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${DISPATCH_TOPIC}" <"${DISPATCH_FILE}"

echo "==> Waiting for ${COUNT} worker completions (timeout ${WAIT_TIMEOUT_SECONDS}s)..."
processed_jobs=0
wait_started="$(date +%s)"
while true; do
  processed_jobs="$(count_processed_jobs)"
  if [[ "${processed_jobs}" -ge "${COUNT}" ]]; then
    break
  fi

  if [[ $(($(date +%s) - wait_started)) -ge "${WAIT_TIMEOUT_SECONDS}" ]]; then
    echo "FAIL: only ${processed_jobs}/${COUNT} completions within ${WAIT_TIMEOUT_SECONDS}s" >&2
    echo "Inspect worker log: ${WORKER_LOG}" >&2
    grep "work_queue_full_errors\|message_errors\|received task job_id=${JOB_PREFIX}" "${WORKER_LOG}" >&2 || true
    exit 1
  fi

  sleep 0.2
done

elapsed_seconds="$(python3 -c "import time; print(time.time() - ${bench_start_time})")"

if [[ "${processed_jobs}" -ne "${generated_jobs}" ]]; then
  echo "FAIL: processed_jobs=${processed_jobs} != generated_jobs=${generated_jobs}" >&2
  exit 1
fi

jobs_processed_per_second="$(python3 -c "
processed = ${processed_jobs}
elapsed = ${elapsed_seconds}
print(processed / elapsed if elapsed > 0 else 0.0)
")"

echo
echo "Worker throughput benchmark finished."
echo "  generated_jobs:             ${generated_jobs}"
echo "  processed_jobs:             ${processed_jobs}"
echo "  elapsed_seconds:            ${elapsed_seconds}"
echo "  jobs_processed_per_second:    ${jobs_processed_per_second}"
echo "  worker_count:               ${WORKERS}"
echo "  queue_capacity:             ${QUEUE_CAPACITY}"
echo
echo "event=benchmark_worker_throughput generated_jobs=${generated_jobs} processed_jobs=${processed_jobs} elapsed_seconds=${elapsed_seconds} jobs_processed_per_second=${jobs_processed_per_second} worker_count=${WORKERS} queue_capacity=${QUEUE_CAPACITY} job_prefix=${JOB_PREFIX}"
