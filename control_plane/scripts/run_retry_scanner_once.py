#!/usr/bin/env python3
"""
Run one retry scan: move due retry_scheduled jobs back to queued.

This is a **manual integration script** for local development. It **only**
requeues jobs whose ``retry_after`` timestamp has passed — it does **not**
dispatch to Kafka, consume results, or create jobs.

Prerequisites:
  - Postgres: ``docker compose up -d postgres`` + migrations applied
  - ``retry_after`` column on ``jobs`` (see migrations / test fixture)
  - Rows in ``retry_scheduled`` with ``retry_after <= now`` if you expect requeues

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/run_retry_scanner_once.py

Or:

    python3 control_plane/scripts/run_retry_scanner_once.py

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
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.retry_scanner import RetryScanner

# Max jobs to requeue in one scan pass (same default as RetryScanner).
MAX_JOBS_PER_SCAN = 100


def _print_summary(result) -> None:
    """Print scan outcome in a readable, copy-paste-friendly format."""
    print("Retry scan finished.")
    print()
    print(f"  scanned_at:       {result.scanned_at}")
    print(f"  requeued_count:   {result.requeued_count}")
    print()
    print("  requeued_job_ids:")
    if result.requeued_job_ids:
        for job_id in result.requeued_job_ids:
            print(f"    - {job_id}")
    else:
        print("    (none — no due retry_scheduled jobs this scan)")
    print()
    print("  errors:")
    if result.errors:
        for message in result.errors:
            print(f"    - {message}")
    else:
        print("    (none)")


def main() -> None:
    # --- Step 1: Postgres connection and repository ---
    with connect() as conn:
        repository = JobRepository(conn)

        # --- Step 2: Retry scanner (requeue due rows only) ---
        scanner = RetryScanner(repository, max_jobs_per_scan=MAX_JOBS_PER_SCAN)

        # --- Step 3: One scan pass (now = current Unix time) ---
        result = scanner.run_once()

        # --- Step 4: Human-readable summary ---
        _print_summary(result)


if __name__ == "__main__":
    main()
