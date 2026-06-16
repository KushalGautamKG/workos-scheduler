#!/usr/bin/env python3
"""
List recent dead-lettered jobs from Postgres for operator inspection.

Dead-lettered jobs are terminal — they will not be retried automatically.
Use this script to see which jobs exhausted retries or were permanently failed.

Prerequisites:
  - Postgres: ``docker compose up -d postgres`` + migrations applied

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/list_dead_lettered_jobs.py

Or:

    python3 control_plane/scripts/list_dead_lettered_jobs.py

(The script adds the repo root to ``sys.path`` automatically.)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Allow running as a file path without installing the package.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository

# How many dead-lettered rows to show (newest ``updated_at`` first).
DEFAULT_LIMIT = 20


def _print_job(job: dict) -> None:
    """Print one dead-lettered job in a readable block."""
    print(f"  job_id:       {job['job_id']}")
    print(f"  tenant_id:    {job['tenant_id']}")
    print(f"  priority:     {job['priority']}")
    print(f"  retries:      {job['retry_count']}/{job['max_retries']}")
    print(f"  updated_at:   {job['updated_at']}")
    print(f"  payload:      {json.dumps(job['payload'], sort_keys=True)}")


def main() -> None:
    # --- Step 1: Postgres connection and repository ---
    with connect() as conn:
        repository = JobRepository(conn)

        # --- Step 2: Load recent dead-lettered jobs ---
        jobs = repository.list_dead_lettered_jobs(limit=DEFAULT_LIMIT)

    # --- Step 3: Human-readable output ---
    if not jobs:
        print("No dead-lettered jobs found.")
        return

    print(f"Dead-lettered jobs (showing up to {DEFAULT_LIMIT}, newest first):")
    print()

    for index, job in enumerate(jobs, start=1):
        print(f"[{index}]")
        _print_job(job)
        print()


if __name__ == "__main__":
    main()
