#!/usr/bin/env python3
"""
Print how many jobs exist in each Postgres lifecycle state.

Use this for a quick operator snapshot of queue depth, failures, and
dead-letter volume without querying SQL by hand.

Prerequisites:
  - Postgres: ``docker compose up -d postgres`` + migrations applied

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/job_state_snapshot.py

Or:

    python3 control_plane/scripts/job_state_snapshot.py

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
from control_plane.kernelq.logging_utils import format_log_event


def _print_structured_summary(counts: dict[str, int]) -> None:
    """Print one grep-friendly line for log collectors and scripts."""
    print(
        format_log_event(
            "job_state_snapshot",
            total_jobs=sum(counts.values()),
            states_count=len(counts),
        )
    )


def main() -> None:
    # --- Step 1: Postgres connection and repository ---
    with connect() as conn:
        repository = JobRepository(conn)

        # --- Step 2: Count jobs grouped by state ---
        counts = repository.count_jobs_by_state()

    # --- Step 3: Human-readable output ---
    print("Job state counts:")

    if not counts:
        print("  (none)")
    else:
        for state in sorted(counts):
            print(f"  {state}: {counts[state]}")

    # Structured summary for grep and log pipelines.
    _print_structured_summary(counts)


if __name__ == "__main__":
    main()
