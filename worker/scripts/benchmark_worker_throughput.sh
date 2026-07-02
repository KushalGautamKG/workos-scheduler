#!/usr/bin/env bash
#
# Benchmark Go worker throughput: produce N dispatch events, wait for matching results.
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
# Completion uses a per-run Kafka consumer group (auto.offset.reset=latest) so only
# NEW results after the seek point are recorded. Exact job_id matching avoids stale
# topic history from prior runs.

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
RESULTS_TOPIC="kernelq.jobs.results"
CONSUMER_BIN="${TMPDIR:-/tmp}/kernelq-consumer-benchmark"
WORKER_LOG="${TMPDIR:-/tmp}/kernelq-worker-benchmark.log"
DISPATCH_FILE="${TMPDIR:-/tmp}/kernelq-worker-benchmark-dispatch.jsonl"
WORKER_PID=""

# Unique per run; job ids are deterministic within the run (00001..COUNT).
RUN_ID="$(date +%s)"
JOB_PREFIX="worker-bench-${RUN_ID}"
RESULTS_CACHE="${TMPDIR:-/tmp}/kernelq-bench-results-${RUN_ID}.txt"
RESULTS_GROUP="kernelq-bench-results-${RUN_ID}"

cleanup() {
  if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
    kill -INT "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
  fi
  rm -f "${DISPATCH_FILE}" "${RESULTS_CACHE}"
}

trap cleanup EXIT

# Append only newly committed results for this benchmark consumer group.
poll_new_results() {
  docker exec "${KAFKA_CONTAINER}" kafka-console-consumer \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --topic "${RESULTS_TOPIC}" \
    --consumer-property "group.id=${RESULTS_GROUP}" \
    --consumer-property "auto.offset.reset=latest" \
    --timeout-ms 400 \
    --max-messages 100 2>/dev/null >>"${RESULTS_CACHE}" || true
}

# Position the benchmark consumer at the topic tail before producing dispatch events.
seek_results_consumer_to_latest() {
  : >"${RESULTS_CACHE}"
  poll_new_results
}

# Count exact job_id matches for this run only (no substring / stale prefix hits).
count_processed_jobs() {
  local processed=0
  local i
  local job_id

  if [[ ! -s "${RESULTS_CACHE}" ]]; then
    echo 0
    return
  fi

  for i in $(seq 1 "${COUNT}"); do
    job_id="$(printf "%s-%05d" "${JOB_PREFIX}" "${i}")"
    if grep -Fq "\"job_id\":\"${job_id}\"" "${RESULTS_CACHE}"; then
      processed=$((processed + 1))
    fi
  done

  echo "${processed}"
}

print_failure_diagnostics() {
  local processed="$1"
  local i
  local job_id
  local missing=0

  echo "FAIL: processed_jobs=${processed} expected=${COUNT} (prefix=${JOB_PREFIX})" >&2
  echo "Missing result job_ids:" >&2
  for i in $(seq 1 "${COUNT}"); do
    job_id="$(printf "%s-%05d" "${JOB_PREFIX}" "${i}")"
    if ! grep -Fq "\"job_id\":\"${job_id}\"" "${RESULTS_CACHE}"; then
      echo "  - ${job_id}" >&2
      missing=$((missing + 1))
    fi
  done
  if [[ "${missing}" -eq 0 ]]; then
    echo "  (none — recount mismatch; inspect ${RESULTS_CACHE})" >&2
  fi
  echo "Worker log: ${WORKER_LOG}" >&2
  grep -E "work_queue_full_errors=|message_errors=|kafka_errors=" "${WORKER_LOG}" 2>/dev/null | tail -5 >&2 || true
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
_startup_deadline=$((SECONDS + 15))
while ! grep -Fq "KernelQ worker consumer started" "${WORKER_LOG}"; do
  if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
    echo "FAIL: worker exited before startup" >&2
    cat "${WORKER_LOG}" >&2
    exit 1
  fi
  if (( SECONDS >= _startup_deadline )); then
    echo "FAIL: worker did not log startup within 15s" >&2
    cat "${WORKER_LOG}" >&2
    exit 1
  fi
  sleep 0.25
done

echo "==> Generating ${COUNT} dispatch events (prefix=${JOB_PREFIX})..."
: >"${DISPATCH_FILE}"
for i in $(seq 1 "${COUNT}"); do
  job_id="$(printf "%s-%05d" "${JOB_PREFIX}" "${i}")"
  printf '%s\n' \
    "{\"event_type\":\"job.dispatch\",\"job_id\":\"${job_id}\",\"tenant_id\":\"tenant-bench\",\"priority\":100,\"state\":\"dispatched\",\"payload\":{\"kind\":\"worker-throughput-bench\"}}" \
    >>"${DISPATCH_FILE}"
done

generated_jobs="${COUNT}"

echo "==> Seeking results consumer to latest (group=${RESULTS_GROUP})..."
seek_results_consumer_to_latest

bench_start_time="$(python3 -c 'import time; print(time.time())')"

echo "==> Producing dispatch batch..."
docker exec -i "${KAFKA_CONTAINER}" kafka-console-producer \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --topic "${DISPATCH_TOPIC}" <"${DISPATCH_FILE}"

echo "==> Waiting for ${COUNT} results (timeout ${WAIT_TIMEOUT_SECONDS}s)..."
processed_jobs=0
wait_started="${SECONDS}"
while true; do
  poll_new_results
  processed_jobs="$(count_processed_jobs)"

  if [[ "${processed_jobs}" -ge "${COUNT}" ]]; then
    break
  fi

  if (( SECONDS - wait_started >= WAIT_TIMEOUT_SECONDS )); then
    print_failure_diagnostics "${processed_jobs}"
    exit 1
  fi

  sleep 0.05
done

elapsed_seconds="$(python3 -c "import time; print(time.time() - ${bench_start_time})")"

if [[ "${processed_jobs}" -ne "${generated_jobs}" ]]; then
  print_failure_diagnostics "${processed_jobs}"
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
