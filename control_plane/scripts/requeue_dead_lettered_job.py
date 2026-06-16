#!/usr/bin/env python3
"""
Manually requeue one dead-lettered job back to ``queued``.

Operator-driven replay only — the job must currently be in ``dead_lettered``
state. ``retry_count`` is preserved; ``retry_after`` is cleared.

Prerequisites:
  - Postgres: ``docker compose up -d postgres`` + migrations applied

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/requeue_dead_lettered_job.py <job_id>

Or:

    python3 control_plane/scripts/requeue_dead_lettered_job.py <job_id>

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

USAGE = "usage: requeue_dead_lettered_job.py <job_id>"


def main() -> None:
    # --- Step 1: Require exactly one job_id argument ---
    if len(sys.argv) != 2 or not sys.argv[1].strip():
        print(USAGE, file=sys.stderr)
        sys.exit(1)

    job_id = sys.argv[1].strip()

    # --- Step 2: Postgres connection and manual requeue ---
    with connect() as conn:
        repository = JobRepository(conn)
        requeued = repository.requeue_dead_lettered_job(job_id)

    # --- Step 3: Outcome for operators ---
    if requeued:
        print(f"requeued job_id={job_id} state=queued")
        return

    print(f"not requeued job_id={job_id} reason=not_found_or_not_dead_lettered")
    sys.exit(1)


if __name__ == "__main__":
    main()
