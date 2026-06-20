#!/usr/bin/env python3
"""
Print average queue wait and completion time for completed jobs in Postgres.

Uses durable ``created_at`` / ``updated_at`` timestamps (and ``dispatched_at``
when present on job rows) via ``compute_job_duration_metrics``.

Prerequisites:
  - Postgres: ``docker compose up -d postgres`` + migrations applied

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/job_duration_snapshot.py

Or:

    python3 control_plane/scripts/job_duration_snapshot.py

(The script adds the repo root to ``sys.path`` automatically.)
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running as a file path without installing the package.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_metrics import compute_job_duration_metrics
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.logging_utils import format_log_event

# Cap rows read on large local datasets (seed data, smoke-test history).
DEFAULT_JOB_LIMIT = 100_000


def _print_structured_summary(metrics) -> None:
    """Print one grep-friendly line for log collectors and scripts."""
    print(
        format_log_event(
            "job_duration_snapshot",
            completed_jobs_count=metrics.completed_jobs_count,
            average_queue_wait_seconds=metrics.average_queue_wait_seconds,
            average_completion_seconds=metrics.average_completion_seconds,
            p50_queue_wait_seconds=metrics.p50_queue_wait_seconds,
            p95_queue_wait_seconds=metrics.p95_queue_wait_seconds,
            p99_queue_wait_seconds=metrics.p99_queue_wait_seconds,
        )
    )


def main() -> None:
    # --- Step 1: Postgres connection and repository ---
    with connect() as conn:
        repository = JobRepository(conn)

        # --- Step 2: Load jobs and compute duration averages ---
        jobs = repository.list_jobs(limit=DEFAULT_JOB_LIMIT)
        metrics = compute_job_duration_metrics(jobs)

    # --- Step 3: Human-readable output ---
    print("Job duration metrics:")
    print(f"  completed_jobs_count: {metrics.completed_jobs_count}")
    print(f"  average_queue_wait_seconds: {metrics.average_queue_wait_seconds}")
    print(f"  average_completion_seconds: {metrics.average_completion_seconds}")
    print(f"  p50_queue_wait_seconds: {metrics.p50_queue_wait_seconds}")
    print(f"  p95_queue_wait_seconds: {metrics.p95_queue_wait_seconds}")
    print(f"  p99_queue_wait_seconds: {metrics.p99_queue_wait_seconds}")

    # Structured summary for grep and log pipelines.
    _print_structured_summary(metrics)


if __name__ == "__main__":
    main()
