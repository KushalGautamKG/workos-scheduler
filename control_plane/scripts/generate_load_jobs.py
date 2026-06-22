#!/usr/bin/env python3
"""
Create many queued jobs in Postgres for local load testing and benchmarking.

Uses ``JobRepository`` directly (no HTTP API). Each job gets a unique id under
``--prefix``, cycles tenants and priorities, and starts in ``queued`` state.

Prerequisites:
  - Postgres: ``docker compose up -d postgres`` + migrations applied

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/generate_load_jobs.py

Examples:

    PYTHONPATH=. python3 control_plane/scripts/generate_load_jobs.py --count 500
    PYTHONPATH=. python3 control_plane/scripts/generate_load_jobs.py --prefix bench --tenants 3
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

# Allow running as a file path without installing the package.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.logging_utils import format_log_event


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Insert queued jobs into Postgres for load testing.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="How many jobs to create (default: 100).",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="load",
        help='Job id prefix (default: "load").',
    )
    parser.add_argument(
        "--tenants",
        type=int,
        default=5,
        help="Number of tenant ids to cycle: tenant-0 .. tenant-(N-1) (default: 5).",
    )
    parser.add_argument(
        "--max-priority",
        type=int,
        default=100,
        help="Highest priority value; jobs cycle 0..max-priority (default: 100).",
    )
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> None:
    """Reject invalid CLI values before touching Postgres."""
    if args.count <= 0:
        raise SystemExit("--count must be a positive integer")
    if args.tenants <= 0:
        raise SystemExit("--tenants must be a positive integer")
    if args.max_priority < 0:
        raise SystemExit("--max-priority must be >= 0")


def _tenant_id(index: int, tenant_count: int) -> str:
    """Cycle tenant-0, tenant-1, ... for fairness-style load mixes."""
    return f"tenant-{index % tenant_count}"


def _priority(index: int, max_priority: int) -> int:
    """Cycle priorities from 0 through max_priority inclusive."""
    if max_priority == 0:
        return 0
    return index % (max_priority + 1)


def main() -> None:
    args = _parse_args()
    _validate_args(args)

    run_timestamp = int(time.time())
    prefix = args.prefix.strip() or "load"

    # --- Step 1: connect and create jobs ---
    started = time.perf_counter()
    created_count = 0

    with connect() as conn:
        repo = JobRepository(conn)

        for index in range(args.count):
            job_id = f"{prefix}-{run_timestamp}-{index}"
            tenant_id = _tenant_id(index, args.tenants)
            priority = _priority(index, args.max_priority)

            repo.create_job(
                job_id=job_id,
                tenant_id=tenant_id,
                priority=priority,
                state=JobState.QUEUED.value,
                payload={"kind": "load-test", "index": index},
            )
            created_count += 1

    elapsed_seconds = time.perf_counter() - started
    jobs_per_second = created_count / elapsed_seconds if elapsed_seconds > 0 else 0.0

    # --- Step 2: human-readable summary ---
    print(f"created_jobs={created_count}")
    print(f"elapsed_seconds={elapsed_seconds}")
    print(f"jobs_per_second={jobs_per_second}")

    # --- Step 3: structured log line for grep and benchmarks ---
    print(
        format_log_event(
            "generate_load_jobs",
            created_jobs=created_count,
            elapsed_seconds=elapsed_seconds,
            jobs_per_second=jobs_per_second,
            tenants=args.tenants,
        )
    )


if __name__ == "__main__":
    main()
