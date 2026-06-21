#!/usr/bin/env python3
"""
Seed completed jobs with realistic queue-wait and completion timestamps.

Creates succeeded rows with ``created_at < dispatched_at < updated_at`` so
duration and percentile metrics have non-zero samples.

Prerequisites:
  - Postgres: ``docker compose up -d postgres``
  - Migrations applied (including ``003_add_dispatched_at.sql``)

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/seed_latency_metrics.py
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.logging_utils import format_log_event

# At least 20 jobs; queue waits cycle through these seconds.
QUEUE_WAIT_SECONDS = (1, 2, 3, 5, 10)
COMPLETION_AFTER_DISPATCH_SECONDS = (2, 3, 5, 8, 12, 15, 20)
JOB_COUNT = 20


def main() -> None:
    run_id = int(time.time())
    created_count = 0

    with connect() as conn:
        repo = JobRepository(conn)

        for index in range(JOB_COUNT):
            job_id = f"seed-latency-{run_id}-{index:02d}"
            queue_wait = QUEUE_WAIT_SECONDS[index % len(QUEUE_WAIT_SECONDS)]
            completion_extra = COMPLETION_AFTER_DISPATCH_SECONDS[
                index % len(COMPLETION_AFTER_DISPATCH_SECONDS)
            ]

            created_at = datetime.now(timezone.utc) - timedelta(hours=24, seconds=index * 11)
            dispatched_at = created_at + timedelta(seconds=queue_wait)
            updated_at = dispatched_at + timedelta(seconds=completion_extra)

            repo.create_job(
                job_id=job_id,
                tenant_id="tenant-a",
                priority=1,
                state=JobState.SUCCEEDED.value,
                payload={"kind": "seed_latency_metrics"},
            )

            conn.execute(
                """
                UPDATE jobs
                SET
                    state = %(state)s,
                    created_at = %(created_at)s,
                    dispatched_at = %(dispatched_at)s,
                    updated_at = %(updated_at)s
                WHERE job_id = %(job_id)s
                """,
                {
                    "job_id": job_id,
                    "state": JobState.SUCCEEDED.value,
                    "created_at": created_at,
                    "dispatched_at": dispatched_at,
                    "updated_at": updated_at,
                },
            )
            conn.commit()
            created_count += 1

    print(f"created_jobs={created_count}")
    print(format_log_event("seed_latency_metrics", created_jobs=created_count))


if __name__ == "__main__":
    main()
