#!/usr/bin/env bash
#
# Smoke test: retryable worker result with exhausted retry budget → dead_lettered.
#
# Validates max retry exhaustion in Postgres only — no Go worker required.
#
# Run from the repository root:
#   ./control_plane/scripts/smoke_retry_exhaustion.sh
#
# Requires: Docker, docker compose, Python 3, and network access to localhost:5432.

set -euo pipefail

# --- Sanity check: this script expects to run from the repo root ---
if [[ ! -f docker-compose.yml ]] || [[ ! -d control_plane/kernelq ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

JOB_ID="day57-exhaust-$(date +%s)"

FINAL_STATE=""
STATE_AFTER_RETRY_SCANNER=""

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

echo "==> 2) Using job_id=${JOB_ID}"

echo "==> 3) Creating dispatched job in Postgres with retry_count=3, max_retries=3..."
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
        payload={"kind": "day57-exhaust"},
        max_retries=3,
    )
    # Simulate a job that has already used all retry attempts.
    conn.execute(
        """
        UPDATE jobs
        SET retry_count = %(retry_count)s, updated_at = NOW()
        WHERE job_id = %(job_id)s
        """,
        {"job_id": job_id, "retry_count": 3},
    )
    conn.commit()
    loaded = repo.get_job(job_id)
    assert loaded is not None
    assert loaded.retry_count == 3
    assert loaded.max_retries == 3
    print(
        f"created job_id={loaded.job_id} state={loaded.state} "
        f"retry_count={loaded.retry_count} max_retries={loaded.max_retries}"
    )
PY

echo "==> 4) Applying retryable_failure through ResultStateHandler..."
FINAL_STATE=$(
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
    message="temporary failure but retries exhausted",
    worker="smoke-test",
)

with connect() as conn:
    repo = JobRepository(conn)
    ResultStateHandler(repo).handle(event)
    loaded = repo.get_job(job_id)
    if loaded is None:
        print("MISSING", file=sys.stderr)
        sys.exit(1)
    if loaded.state != JobState.DEAD_LETTERED.value:
        print(f"unexpected state={loaded.state}", file=sys.stderr)
        sys.exit(1)
    print(loaded.state)
PY
)

echo "==> 5) Running retry scanner once (dead_lettered must not requeue)..."
PYTHONPATH=. python3 control_plane/scripts/run_retry_scanner_once.py

echo "==> 6) Verifying state remains dead_lettered after scanner..."
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
    if loaded.state != JobState.DEAD_LETTERED.value:
        print(f"unexpected state={loaded.state}", file=sys.stderr)
        sys.exit(1)
    print(loaded.state)
PY
)

echo ""
echo "==> Summary"
echo "job_id=${JOB_ID}"
echo "final_state=${FINAL_STATE}"
echo "state_after_retry_scanner=${STATE_AFTER_RETRY_SCANNER}"

if [[ "${FINAL_STATE}" != "dead_lettered" ]]; then
  echo "FAIL: expected final_state=dead_lettered" >&2
  exit 1
fi
if [[ "${STATE_AFTER_RETRY_SCANNER}" != "dead_lettered" ]]; then
  echo "FAIL: expected state_after_retry_scanner=dead_lettered" >&2
  exit 1
fi

echo "PASS: retry exhaustion smoke test succeeded for job_id=${JOB_ID}"
exit 0
