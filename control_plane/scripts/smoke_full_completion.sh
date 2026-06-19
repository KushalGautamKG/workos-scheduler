#!/usr/bin/env bash
#
# End-to-end smoke test: Postgres job → scheduler dispatch → Go worker →
# Kafka result → Python result consumer → Postgres state = succeeded.
#
# Run from the repository root:
#   ./control_plane/scripts/smoke_full_completion.sh
#
# Requires: Docker, docker compose, Go, Python 3, and network access to
# localhost:5432 and localhost:9092.

set -euo pipefail

# Print one grep-friendly summary line (error values JSON-quoted when needed).
log_smoke_summary() {
  local success="$1"
  local job_id="${2:-unknown}"
  local final_state="${3:-unknown}"
  local error_msg="${4:-}"

  if [[ -z "${job_id}" ]]; then
    job_id="unknown"
  fi
  if [[ -z "${final_state}" ]]; then
    final_state="unknown"
  fi

  if [[ "${success}" == "true" ]]; then
    echo "event=smoke_full_completion job_id=${job_id} final_state=${final_state} success=true"
    return
  fi

  local quoted_error
  quoted_error=$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "${error_msg}")
  echo "event=smoke_full_completion job_id=${job_id} final_state=${final_state} success=false error=${quoted_error}"
}

# --- Sanity check: this script expects to run from the repo root ---
if [[ ! -f docker-compose.yml ]] || [[ ! -d control_plane/kernelq ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  log_smoke_summary false unknown unknown "Run this script from the repository root."
  exit 1
fi

WORKER_LOG="/tmp/kernelq-full-worker.log"
WORKER_PID=""

# Unique job id so we can tell when *our* job finished (not an old Kafka message).
JOB_ID="day52-full-$(date +%s)"

# How long to keep polling results / checking Postgres (seconds).
RESULT_WAIT_SECONDS=90

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

echo "==> 1) Starting Postgres, Zookeeper, and Kafka..."
docker compose up -d postgres zookeeper kafka

echo "==> Waiting for Postgres to accept connections..."
sleep 3
docker exec kernelq-postgres pg_isready -U kernelq -d kernelq

echo "==> Ensuring jobs table exists (migration 001, safe to re-run)..."
docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
  < control_plane/migrations/001_create_jobs.sql

echo "==> 2) Creating Kafka topics..."
./infra/kafka/create-topics.sh

echo "==> 3) Building worker binary..."
(
  cd worker
  go build -o consumer ./cmd/consumer
)

echo "==> 4) Using job_id=${JOB_ID}"

echo "==> 5) Creating queued job in Postgres..."
PYTHONPATH=. python3 - <<PY
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState

job_id = "${JOB_ID}"

with connect() as conn:
    repo = JobRepository(conn)
    record = repo.create_job(
        job_id=job_id,
        tenant_id="tenant-a",
        priority=999999,
        state=JobState.QUEUED.value,
        payload={"kind": "day52-full"},
    )
    print(f"created job_id={record.job_id} state={record.state}")
PY

echo "==> 6) Starting Go worker in background..."
./worker/consumer > "${WORKER_LOG}" 2>&1 &
WORKER_PID=$!
echo "Worker PID: ${WORKER_PID}"

# Give the consumer time to connect and subscribe to kernelq.jobs.dispatch.
echo "==> Waiting for worker to start..."
sleep 5

echo "==> 7) Running one scheduler tick (claim + publish to Kafka)..."
PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py

# Give the worker time to execute and publish a result event.
echo "==> Waiting for worker to process dispatch..."
sleep 3

echo "==> 8) Consuming result messages until ${JOB_ID} is succeeded..."
echo "    (Older Kafka messages may fail or update other jobs — we keep trying.)"

FINAL_STATE="unknown"
SECONDS=0

while [[ "${SECONDS}" -lt "${RESULT_WAIT_SECONDS}" ]]; do
  # poll_once reads at most one message; stale records may error — that is OK.
  PYTHONPATH=. python3 control_plane/scripts/consume_result_once.py || true

  FINAL_STATE=$(
    PYTHONPATH=. python3 - <<PY
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository

job_id = "${JOB_ID}"

with connect() as conn:
    repo = JobRepository(conn)
    job = repo.get_job(job_id)
    print(job.state if job is not None else "missing")
PY
  )

  echo "  check: job_id=${JOB_ID} state=${FINAL_STATE} (elapsed ${SECONDS}s)"

  if [[ "${FINAL_STATE}" == "succeeded" ]]; then
    break
  fi

  sleep 2
  SECONDS=$((SECONDS + 2))
done

echo ""
echo "==> 9) Final Postgres state for ${JOB_ID}:"
echo "final_state=${FINAL_STATE}"

if [[ "${FINAL_STATE}" != "succeeded" ]]; then
  echo "FAIL: expected final_state=succeeded for job_id=${JOB_ID}" >&2
  log_smoke_summary false "${JOB_ID}" "${FINAL_STATE}" "expected final_state=succeeded"
  exit 1
fi

echo "PASS: full completion loop succeeded for job_id=${JOB_ID}"
log_smoke_summary true "${JOB_ID}" "${FINAL_STATE}"
exit 0
