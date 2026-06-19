#!/usr/bin/env bash
#
# Smoke test: retryable worker result → retry_scheduled → queued → dispatched.
#
# Validates Postgres retry state movement only — no Go worker required.
#
# Run from the repository root:
#   ./control_plane/scripts/smoke_retry_requeue.sh
#
# Requires: Docker, docker compose, Python 3, and network access to
# localhost:5432 (and localhost:9092 for the scheduler publish step).

set -euo pipefail

# Print one grep-friendly summary line (error values JSON-quoted when needed).
log_smoke_summary() {
  local success="$1"
  local job_id="${2:-unknown}"
  local state_after_retry_result="${3:-unknown}"
  local state_after_retry_scanner="${4:-unknown}"
  local state_after_scheduler_tick="${5:-unknown}"
  local error_msg="${6:-}"

  [[ -z "${job_id}" ]] && job_id="unknown"
  [[ -z "${state_after_retry_result}" ]] && state_after_retry_result="unknown"
  [[ -z "${state_after_retry_scanner}" ]] && state_after_retry_scanner="unknown"
  [[ -z "${state_after_scheduler_tick}" ]] && state_after_scheduler_tick="unknown"

  if [[ "${success}" == "true" ]]; then
    echo "event=smoke_retry_requeue job_id=${job_id} state_after_retry_result=${state_after_retry_result} state_after_retry_scanner=${state_after_retry_scanner} state_after_scheduler_tick=${state_after_scheduler_tick} success=true"
    return
  fi

  local quoted_error
  quoted_error=$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "${error_msg}")
  echo "event=smoke_retry_requeue job_id=${job_id} state_after_retry_result=${state_after_retry_result} state_after_retry_scanner=${state_after_retry_scanner} state_after_scheduler_tick=${state_after_scheduler_tick} success=false error=${quoted_error}"
}

# --- Sanity check: this script expects to run from the repo root ---
if [[ ! -f docker-compose.yml ]] || [[ ! -d control_plane/kernelq ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  log_smoke_summary false unknown unknown unknown unknown "Run this script from the repository root."
  exit 1
fi

JOB_ID="day55-retry-$(date +%s)"

STATE_AFTER_RETRY_RESULT=""
STATE_AFTER_RETRY_SCANNER=""
STATE_AFTER_SCHEDULER_TICK=""

cleanup() {
  # Best-effort delete so repeated runs stay tidy.
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

echo "==> Ensuring jobs table exists (migration 001, safe to re-run)..."
docker exec -i kernelq-postgres psql -U kernelq -d kernelq \
  < control_plane/migrations/001_create_jobs.sql

echo "==> Ensuring retry_after column exists..."
docker exec -i kernelq-postgres psql -U kernelq -d kernelq -c \
  "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS retry_after BIGINT;"

echo "==> 2) Creating Kafka topics..."
./infra/kafka/create-topics.sh

echo "==> 3) Using job_id=${JOB_ID}"

echo "==> 4) Creating dispatched job in Postgres..."
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
        state=JobState.DISPATCHED.value,
        payload={"kind": "day55-retry"},
        max_retries=3,
    )
    assert record.retry_count == 0
    assert record.max_retries == 3
    print(f"created job_id={record.job_id} state={record.state} retry_count={record.retry_count}")
PY

echo "==> 5) Applying retryable_failure through ResultStateHandler..."
STATE_AFTER_RETRY_RESULT=$(
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
    status="retryable_failure",
    message="temporary failure",
    worker="smoke-test",
)

with connect() as conn:
    repo = JobRepository(conn)
    ResultStateHandler(repo).handle(event)
    loaded = repo.get_job(job_id)
    if loaded is None:
        print("MISSING", file=sys.stderr)
        sys.exit(1)
    if loaded.state != JobState.RETRY_SCHEDULED.value:
        print(f"unexpected state={loaded.state}", file=sys.stderr)
        sys.exit(1)
    print(loaded.state)
PY
)

echo "==> 6) Making retry due immediately (retry_after = now - 1)..."
PYTHONPATH=. python3 - <<PY
import time
from control_plane.kernelq.db import connect

job_id = "${JOB_ID}"
due_at = int(time.time()) - 1

with connect() as conn:
    conn.execute(
        """
        UPDATE jobs
        SET retry_after = %(retry_after)s, updated_at = NOW()
        WHERE job_id = %(job_id)s
        """,
        {"job_id": job_id, "retry_after": due_at},
    )
    conn.commit()
print(f"set retry_after={due_at} for job_id={job_id}")
PY

echo "==> 7) Running retry scanner once..."
PYTHONPATH=. python3 control_plane/scripts/run_retry_scanner_once.py

echo "==> 8) Verifying state is queued after scanner..."
STATE_AFTER_RETRY_SCANNER=$(
  PYTHONPATH=. python3 - <<PY
import sys
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState

job_id = "${JOB_ID}"

with connect() as conn:
    loaded = JobRepository(conn).get_job(job_id)
    if loaded is None:
        print("MISSING", file=sys.stderr)
        sys.exit(1)
    if loaded.state != JobState.QUEUED.value:
        print(f"unexpected state={loaded.state}", file=sys.stderr)
        sys.exit(1)
    print(loaded.state)
PY
)

echo "==> 9) Running scheduler tick once..."
PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py

echo "==> 10) Verifying state is dispatched after scheduler tick..."
STATE_AFTER_SCHEDULER_TICK=$(
  PYTHONPATH=. python3 - <<PY
import sys
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState

job_id = "${JOB_ID}"

with connect() as conn:
    loaded = JobRepository(conn).get_job(job_id)
    if loaded is None:
        print("MISSING", file=sys.stderr)
        sys.exit(1)
    if loaded.state != JobState.DISPATCHED.value:
        print(f"unexpected state={loaded.state}", file=sys.stderr)
        sys.exit(1)
    print(loaded.state)
PY
)

echo ""
echo "==> Summary"
echo "job_id=${JOB_ID}"
echo "state_after_retry_result=${STATE_AFTER_RETRY_RESULT}"
echo "state_after_retry_scanner=${STATE_AFTER_RETRY_SCANNER}"
echo "state_after_scheduler_tick=${STATE_AFTER_SCHEDULER_TICK}"

if [[ "${STATE_AFTER_RETRY_RESULT}" != "retry_scheduled" ]]; then
  echo "FAIL: expected state_after_retry_result=retry_scheduled" >&2
  log_smoke_summary false "${JOB_ID}" "${STATE_AFTER_RETRY_RESULT}" "${STATE_AFTER_RETRY_SCANNER}" "${STATE_AFTER_SCHEDULER_TICK}" "expected state_after_retry_result=retry_scheduled"
  exit 1
fi
if [[ "${STATE_AFTER_RETRY_SCANNER}" != "queued" ]]; then
  echo "FAIL: expected state_after_retry_scanner=queued" >&2
  log_smoke_summary false "${JOB_ID}" "${STATE_AFTER_RETRY_RESULT}" "${STATE_AFTER_RETRY_SCANNER}" "${STATE_AFTER_SCHEDULER_TICK}" "expected state_after_retry_scanner=queued"
  exit 1
fi
if [[ "${STATE_AFTER_SCHEDULER_TICK}" != "dispatched" ]]; then
  echo "FAIL: expected state_after_scheduler_tick=dispatched" >&2
  log_smoke_summary false "${JOB_ID}" "${STATE_AFTER_RETRY_RESULT}" "${STATE_AFTER_RETRY_SCANNER}" "${STATE_AFTER_SCHEDULER_TICK}" "expected state_after_scheduler_tick=dispatched"
  exit 1
fi

echo "PASS: retry requeue smoke test succeeded for job_id=${JOB_ID}"
log_smoke_summary true "${JOB_ID}" "${STATE_AFTER_RETRY_RESULT}" "${STATE_AFTER_RETRY_SCANNER}" "${STATE_AFTER_SCHEDULER_TICK}"
exit 0
