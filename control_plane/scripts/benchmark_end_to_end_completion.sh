#!/usr/bin/env bash
#
# End-to-end completion benchmark: queued jobs in Postgres → scheduler dispatch →
# Go worker → Kafka results → Python result consumer → Postgres succeeded.
#
# Run from the repository root:
#   ./control_plane/scripts/benchmark_end_to_end_completion.sh
#
# Environment (defaults):
#   COUNT=10
#   WORKERS=4
#   QUEUE_CAPACITY=100
#   TRIALS=1
#   TIMEOUT_SECONDS=180
#
# Requires: Docker, Go, Python 3, Postgres and Kafka on localhost.

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d control_plane/kernelq ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

COUNT="${COUNT:-10}"
WORKERS="${WORKERS:-4}"
QUEUE_CAPACITY="${QUEUE_CAPACITY:-100}"
TRIALS="${TRIALS:-1}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-180}"

_validate_positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "${value}" =~ ^[0-9]+$ ]] || (( value <= 0 )); then
    echo "ERROR: ${name} must be a positive integer (got: ${value})" >&2
    exit 1
  fi
}

_validate_positive_int "COUNT" "${COUNT}"
_validate_positive_int "WORKERS" "${WORKERS}"
_validate_positive_int "QUEUE_CAPACITY" "${QUEUE_CAPACITY}"
_validate_positive_int "TRIALS" "${TRIALS}"
_validate_positive_int "TIMEOUT_SECONDS" "${TIMEOUT_SECONDS}"

BASE_RUN_ID="$(date +%s)"
JOB_PREFIX=""
RESULT_GROUP=""
CONSUMER_BIN="${TMPDIR:-/tmp}/kernelq-consumer-e2e-bench"
WORKER_LOG="${TMPDIR:-/tmp}/kernelq-worker-e2e-bench.log"
WORKER_PID=""

cleanup() {
  if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
    kill -INT "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
  fi
}

trap cleanup EXIT

count_jobs_for_prefix() {
  local state="${1:-}"
  PYTHONPATH=. python3 -c "
import os
import sys
from control_plane.kernelq.db import connect

prefix = os.environ['JOB_PREFIX']
state = sys.argv[1] if len(sys.argv) > 1 and sys.argv[1] else None

with connect() as conn:
    with conn.cursor() as cur:
        if state:
            cur.execute(
                \"SELECT COUNT(*) FROM jobs WHERE job_id LIKE %s AND state = %s\",
                (prefix + '-%', state),
            )
        else:
            cur.execute(
                \"SELECT COUNT(*) FROM jobs WHERE job_id LIKE %s\",
                (prefix + '-%',),
            )
        print(cur.fetchone()[0])
" "${state}"
}

fail_benchmark() {
  local message="$1"
  echo "FAIL: ${message}" >&2
  echo "  job_prefix=${JOB_PREFIX}" >&2
  echo "  generated_jobs=${COUNT}" >&2
  echo "  dispatched_jobs=$(count_jobs_for_prefix dispatched)" >&2
  echo "  succeeded_jobs=$(count_jobs_for_prefix succeeded)" >&2
  echo "  worker_log=${WORKER_LOG}" >&2
  exit 1
}

run_single_trial() {
  local trial_number="$1"
  local generated_jobs dispatched_jobs succeeded_jobs elapsed_seconds jobs_completed_per_second

  JOB_PREFIX="e2e-bench-${BASE_RUN_ID}-trial-${trial_number}"
  export JOB_PREFIX
  RESULT_GROUP="kernelq-e2e-bench-${BASE_RUN_ID}-trial-${trial_number}"

  if (( TRIALS > 1 )); then
    echo "==> Trial ${trial_number}/${TRIALS}: creating ${COUNT} queued jobs (prefix=${JOB_PREFIX})..."
  else
    echo "==> Creating ${COUNT} queued jobs (prefix=${JOB_PREFIX})..."
  fi

  PYTHONPATH=. COUNT="${COUNT}" python3 <<'PY'
import os

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState

prefix = os.environ["JOB_PREFIX"]
count = int(os.environ["COUNT"])

with connect() as conn:
    repo = JobRepository(conn)
    for index in range(1, count + 1):
        job_id = f"{prefix}-{index:05d}"
        repo.create_job(
            job_id=job_id,
            tenant_id="tenant-e2e-bench",
            priority=999_999,
            state=JobState.QUEUED.value,
            payload={"kind": "e2e-bench"},
        )
print(f"created_jobs={count}")
PY

  generated_jobs="${COUNT}"
  bench_start_time="$(python3 -c 'import time; print(time.time())')"

  echo "==> Running scheduler ticks until all jobs are dispatched..."
  PYTHONPATH=. COUNT="${COUNT}" python3 <<'PY'
import os
import sys

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.kafka_producer import DEFAULT_BOOTSTRAP_SERVERS, KafkaJobProducer
from control_plane.kernelq.scheduler_tick import SchedulerTickRunner

prefix = os.environ["JOB_PREFIX"]
count = int(os.environ["COUNT"])

def count_queued(cur) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM jobs WHERE job_id LIKE %s AND state = %s",
        (prefix + "-%", "queued"),
    )
    return int(cur.fetchone()[0])

with connect() as conn:
    repo = JobRepository(conn)
    producer = KafkaJobProducer(bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS)
    try:
        runner = SchedulerTickRunner(
            repo,
            max_jobs_per_tick=count,
            job_producer=producer,
        )
        max_iterations = max(count * 2, 10)
        for _ in range(max_iterations):
            with conn.cursor() as cur:
                if count_queued(cur) == 0:
                    break
            result = runner.run_once()
            if result.publish_errors:
                for message in result.publish_errors:
                    print(message, file=sys.stderr)
                raise SystemExit(1)
        with conn.cursor() as cur:
            remaining = count_queued(cur)
        if remaining > 0:
            print(f"FAIL: {remaining} jobs still queued", file=sys.stderr)
            raise SystemExit(1)
    finally:
        producer.close()
PY

  dispatched_jobs="$(count_jobs_for_prefix dispatched)"
  succeeded_after_dispatch="$(count_jobs_for_prefix succeeded)"
  dispatched_jobs=$((dispatched_jobs + succeeded_after_dispatch))

  if [[ "${dispatched_jobs}" -ne "${generated_jobs}" ]]; then
    fail_benchmark "dispatched_jobs=${dispatched_jobs} expected=${generated_jobs}"
  fi

  echo "==> Consuming results until all jobs reach succeeded (timeout ${TIMEOUT_SECONDS}s)..."
  export RESULT_GROUP
  PYTHONPATH=. COUNT="${COUNT}" TIMEOUT_SECONDS="${TIMEOUT_SECONDS}" python3 <<'PY'
import os
import sys
import time

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.kafka_result_consumer import KafkaResultConsumer
from control_plane.kernelq.result_consumer import ResultConsumerRunner
from control_plane.kernelq.result_handler import ResultStateHandler

prefix = os.environ["JOB_PREFIX"]
count = int(os.environ["COUNT"])
timeout_seconds = int(os.environ["TIMEOUT_SECONDS"])
group_id = os.environ["RESULT_GROUP"]

def count_succeeded(cur) -> int:
    cur.execute(
        "SELECT COUNT(*) FROM jobs WHERE job_id LIKE %s AND state = %s",
        (prefix + "-%", "succeeded"),
    )
    return int(cur.fetchone()[0])

deadline = time.time() + timeout_seconds
kafka_consumer = None

with connect() as conn:
    repo = JobRepository(conn)
    handler = ResultStateHandler(repo)
    runner = ResultConsumerRunner(handler)
    kafka_consumer = KafkaResultConsumer(group_id=group_id, runner=runner)

    try:
        while time.time() < deadline:
            try:
                kafka_consumer.poll_once(timeout_seconds=0.5)
            except Exception as exc:
                print(f"result_consumer_error={exc}", file=sys.stderr)

            with conn.cursor() as cur:
                if count_succeeded(cur) >= count:
                    break
        else:
            with conn.cursor() as cur:
                succeeded = count_succeeded(cur)
            print(
                f"FAIL: succeeded_jobs={succeeded} expected={count} (timeout)",
                file=sys.stderr,
            )
            raise SystemExit(1)
    finally:
        kafka_consumer.close()
PY

  elapsed_seconds="$(python3 -c "import time; print(time.time() - ${bench_start_time})")"
  succeeded_jobs="$(count_jobs_for_prefix succeeded)"
  dispatched_jobs="$(count_jobs_for_prefix dispatched)"
  dispatched_jobs=$((dispatched_jobs + succeeded_jobs))

  if [[ "${succeeded_jobs}" -ne "${generated_jobs}" ]]; then
    fail_benchmark "succeeded_jobs=${succeeded_jobs} expected=${generated_jobs}"
  fi

  if [[ "${dispatched_jobs}" -ne "${generated_jobs}" ]]; then
    fail_benchmark "dispatched_jobs=${dispatched_jobs} expected=${generated_jobs}"
  fi

  jobs_completed_per_second="$(python3 -c "
completed = ${succeeded_jobs}
elapsed = ${elapsed_seconds}
print(completed / elapsed if elapsed > 0 else 0.0)
")"

  if (( TRIALS > 1 )); then
    echo "Trial ${trial_number} (${JOB_PREFIX})"
    echo "  generated_jobs:           ${generated_jobs}"
    echo "  dispatched_jobs:          ${dispatched_jobs}"
    echo "  succeeded_jobs:           ${succeeded_jobs}"
    echo "  elapsed_seconds:          ${elapsed_seconds}"
    echo "  jobs_completed_per_second:  ${jobs_completed_per_second}"
    echo
  fi

  TRIAL_GENERATED_JOBS+=("${generated_jobs}")
  TRIAL_DISPATCHED_JOBS+=("${dispatched_jobs}")
  TRIAL_SUCCEEDED_JOBS+=("${succeeded_jobs}")
  TRIAL_ELAPSED_SECONDS+=("${elapsed_seconds}")
  TRIAL_JOBS_PER_SECOND+=("${jobs_completed_per_second}")
  TRIAL_PREFIXES+=("${JOB_PREFIX}")

  if (( TRIALS == 1 )); then
    SINGLE_TRIAL_GENERATED_JOBS="${generated_jobs}"
    SINGLE_TRIAL_DISPATCHED_JOBS="${dispatched_jobs}"
    SINGLE_TRIAL_SUCCEEDED_JOBS="${succeeded_jobs}"
    SINGLE_TRIAL_ELAPSED_SECONDS="${elapsed_seconds}"
    SINGLE_TRIAL_JOBS_PER_SECOND="${jobs_completed_per_second}"
    SINGLE_TRIAL_PREFIX="${JOB_PREFIX}"
  fi
}

echo "==> Starting Postgres, Zookeeper, and Kafka..."
docker compose up -d postgres zookeeper kafka

echo "==> Waiting for Postgres..."
sleep 3
docker exec kernelq-postgres pg_isready -U kernelq -d kernelq

echo "==> Ensuring jobs table exists..."
docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
  < control_plane/migrations/001_create_jobs.sql >/dev/null

echo "==> Creating Kafka topics..."
./infra/kafka/create-topics.sh

echo "==> Building worker..."
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

TRIAL_GENERATED_JOBS=()
TRIAL_DISPATCHED_JOBS=()
TRIAL_SUCCEEDED_JOBS=()
TRIAL_ELAPSED_SECONDS=()
TRIAL_JOBS_PER_SECOND=()
TRIAL_PREFIXES=()

for trial_number in $(seq 1 "${TRIALS}"); do
  run_single_trial "${trial_number}"
done

if (( TRIALS == 1 )); then
  echo
  echo "End-to-end completion benchmark finished."
  echo "  generated_jobs:           ${SINGLE_TRIAL_GENERATED_JOBS}"
  echo "  dispatched_jobs:          ${SINGLE_TRIAL_DISPATCHED_JOBS}"
  echo "  succeeded_jobs:           ${SINGLE_TRIAL_SUCCEEDED_JOBS}"
  echo "  elapsed_seconds:          ${SINGLE_TRIAL_ELAPSED_SECONDS}"
  echo "  jobs_completed_per_second:  ${SINGLE_TRIAL_JOBS_PER_SECOND}"
  echo "  worker_count:             ${WORKERS}"
  echo "  queue_capacity:           ${QUEUE_CAPACITY}"
  echo "  job_prefix:               ${SINGLE_TRIAL_PREFIX}"
  echo
  echo "event=benchmark_end_to_end_completion generated_jobs=${SINGLE_TRIAL_GENERATED_JOBS} dispatched_jobs=${SINGLE_TRIAL_DISPATCHED_JOBS} succeeded_jobs=${SINGLE_TRIAL_SUCCEEDED_JOBS} elapsed_seconds=${SINGLE_TRIAL_ELAPSED_SECONDS} jobs_completed_per_second=${SINGLE_TRIAL_JOBS_PER_SECOND} worker_count=${WORKERS} queue_capacity=${QUEUE_CAPACITY} job_prefix=${SINGLE_TRIAL_PREFIX}"
else
  _rates_csv="$(IFS=,; echo "${TRIAL_JOBS_PER_SECOND[*]}")"
  _elapsed_csv="$(IFS=,; echo "${TRIAL_ELAPSED_SECONDS[*]}")"
  _succeeded_csv="$(IFS=,; echo "${TRIAL_SUCCEEDED_JOBS[*]}")"
  read -r total_succeeded_jobs min_jobs_completed_per_second avg_jobs_completed_per_second max_jobs_completed_per_second min_elapsed_seconds avg_elapsed_seconds max_elapsed_seconds <<<"$(python3 -c "
succeeded = [int(x) for x in '${_succeeded_csv}'.split(',')]
rates = [float(x) for x in '${_rates_csv}'.split(',')]
elapsed = [float(x) for x in '${_elapsed_csv}'.split(',')]
total = sum(succeeded)
print(
    total,
    min(rates),
    sum(rates) / len(rates),
    max(rates),
    min(elapsed),
    sum(elapsed) / len(elapsed),
    max(elapsed),
)
")"

  echo "End-to-end completion benchmark finished."
  echo "  trials:                           ${TRIALS}"
  echo "  generated_jobs_per_trial:         ${COUNT}"
  echo "  total_succeeded_jobs:             ${total_succeeded_jobs}"
  echo "  min_jobs_completed_per_second:    ${min_jobs_completed_per_second}"
  echo "  avg_jobs_completed_per_second:    ${avg_jobs_completed_per_second}"
  echo "  max_jobs_completed_per_second:    ${max_jobs_completed_per_second}"
  echo "  min_elapsed_seconds:              ${min_elapsed_seconds}"
  echo "  avg_elapsed_seconds:              ${avg_elapsed_seconds}"
  echo "  max_elapsed_seconds:              ${max_elapsed_seconds}"
  echo "  worker_count:                     ${WORKERS}"
  echo "  queue_capacity:                   ${QUEUE_CAPACITY}"
  echo
  echo "event=benchmark_end_to_end_completion trials=${TRIALS} generated_jobs_per_trial=${COUNT} total_succeeded_jobs=${total_succeeded_jobs} min_jobs_completed_per_second=${min_jobs_completed_per_second} avg_jobs_completed_per_second=${avg_jobs_completed_per_second} max_jobs_completed_per_second=${max_jobs_completed_per_second} worker_count=${WORKERS} queue_capacity=${QUEUE_CAPACITY}"
fi
