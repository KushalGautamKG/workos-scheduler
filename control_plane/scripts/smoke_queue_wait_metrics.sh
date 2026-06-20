#!/usr/bin/env bash
#
# Smoke test: queued job → sleep → scheduler dispatch → result handler →
# verify per-job queue wait (dispatched_at - created_at) > 0.
#
# Run from the repository root:
#   ./control_plane/scripts/smoke_queue_wait_metrics.sh
#
# Requires: Docker, docker compose, Python 3, and network access to
# localhost:5432 and localhost:9092.

set -euo pipefail

log_smoke_summary() {
  local success="$1"
  local job_id="${2:-unknown}"
  local queue_wait_seconds="${3:-0}"
  local error_msg="${4:-}"

  [[ -z "${job_id}" ]] && job_id="unknown"

  if [[ "${success}" == "true" ]]; then
    PYTHONPATH=. python3 - <<PY
from control_plane.kernelq.logging_utils import format_log_event

print(
    format_log_event(
        "smoke_queue_wait_metrics",
        job_id="${job_id}",
        queue_wait_seconds=${queue_wait_seconds},
        success=True,
    )
)
PY
    return
  fi

  local quoted_error
  quoted_error=$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "${error_msg}")
  PYTHONPATH=. python3 - <<PY
from control_plane.kernelq.logging_utils import format_log_event

print(
    format_log_event(
        "smoke_queue_wait_metrics",
        job_id="${job_id}",
        queue_wait_seconds=${queue_wait_seconds},
        success=False,
        error=${quoted_error},
    )
)
PY
}

if [[ ! -f docker-compose.yml ]] || [[ ! -d control_plane/kernelq ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  log_smoke_summary false unknown 0 "Run this script from the repository root."
  exit 1
fi

JOB_ID="day69-queue-wait-$(date +%s)"
QUEUE_WAIT_SECONDS="0"

cleanup() {
  PYTHONPATH=. python3 - <<PY || true
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository

with connect() as conn:
    JobRepository(conn).delete_job("${JOB_ID}")
PY
}

trap cleanup EXIT

echo "==> 1) Starting Postgres, Zookeeper, and Kafka..."
docker compose up -d postgres zookeeper kafka

echo "==> Waiting for Postgres to accept connections..."
sleep 3
docker exec kernelq-postgres pg_isready -U kernelq -d kernelq

echo "==> Ensuring jobs table and dispatched_at column exist..."
docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
  < control_plane/migrations/001_create_jobs.sql
docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
  < control_plane/migrations/003_add_dispatched_at.sql
docker exec -i kernelq-postgres psql -U kernelq -d kernelq -c \
  "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_after BIGINT;"

echo "==> 2) Creating Kafka topics..."
./infra/kafka/create-topics.sh

echo "==> 3) Using job_id=${JOB_ID}"

echo "==> 4) Creating queued job in Postgres..."
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
        payload={"kind": "day69-queue-wait"},
    )
    print(f"created job_id={record.job_id} state={record.state}")
PY

echo "==> 5) Sleeping 2 seconds before dispatch..."
sleep 2

echo "==> 6) Running one scheduler tick (claim + publish to Kafka)..."
PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py

echo "==> 7) Verifying job was dispatched with dispatched_at..."
PYTHONPATH=. python3 - <<PY
import sys
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState

job_id = "${JOB_ID}"

with connect() as conn:
    loaded = JobRepository(conn).get_job(job_id)
    if loaded is None:
        print("missing job", file=sys.stderr)
        sys.exit(1)
    if loaded.state != JobState.DISPATCHED.value:
        print(f"expected dispatched, got {loaded.state}", file=sys.stderr)
        sys.exit(1)
    if loaded.dispatched_at is None:
        print("dispatched_at is null", file=sys.stderr)
        sys.exit(1)
    print(f"state={loaded.state} dispatched_at={loaded.dispatched_at}")
PY

echo "==> 8) Applying succeeded through ResultStateHandler..."
PYTHONPATH=. python3 - <<PY
import sys
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.result_event import WORKER_RESULT_EVENT_TYPE, WorkerResultEvent
from control_plane.kernelq.result_handler import ResultStateHandler

job_id = "${JOB_ID}"

event = WorkerResultEvent(
    event_type=WORKER_RESULT_EVENT_TYPE,
    job_id=job_id,
    status="succeeded",
    message="smoke queue wait",
    worker="smoke-test",
)

with connect() as conn:
    repo = JobRepository(conn)
    ResultStateHandler(repo).handle(event)
    loaded = repo.get_job(job_id)
    if loaded is None or loaded.state != JobState.SUCCEEDED.value:
        state = loaded.state if loaded else "missing"
        print(f"expected succeeded, got {state}", file=sys.stderr)
        sys.exit(1)
    print(f"final_state={loaded.state}")
PY

echo "==> 9) Running job duration snapshot..."
PYTHONPATH=. python3 control_plane/scripts/job_duration_snapshot.py

echo "==> 10) Computing queue wait for ${JOB_ID}..."
QUEUE_WAIT_SECONDS=$(
  PYTHONPATH=. python3 - <<PY
import sys
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_metrics import compute_job_duration_metrics
from control_plane.kernelq.job_repository import JobRepository

job_id = "${JOB_ID}"

with connect() as conn:
    job = JobRepository(conn).get_job(job_id)
    if job is None:
        print("missing job", file=sys.stderr)
        sys.exit(1)

metrics = compute_job_duration_metrics([job])
print(metrics.average_queue_wait_seconds)
PY
)

if python3 - <<PY
import sys
value = float("${QUEUE_WAIT_SECONDS}")
sys.exit(0 if value > 0 else 1)
PY
then
  :
else
  echo "FAIL: expected queue_wait_seconds > 0, got ${QUEUE_WAIT_SECONDS}" >&2
  log_smoke_summary false "${JOB_ID}" "${QUEUE_WAIT_SECONDS}" "expected queue_wait_seconds > 0"
  exit 1
fi

echo ""
echo "job_id=${JOB_ID}"
echo "queue_wait_seconds=${QUEUE_WAIT_SECONDS}"
echo "PASS"
log_smoke_summary true "${JOB_ID}" "${QUEUE_WAIT_SECONDS}"
exit 0
