#!/usr/bin/env python3
"""
Benchmark scheduler dispatch throughput against Postgres.

This script:
  1. Inserts ``--count`` jobs in ``queued`` state (like ``generate_load_jobs.py``).
  2. Runs ``SchedulerTickRunner`` in a loop until our generated jobs are
     ``dispatched``, we make no progress, or a safety iteration cap is hit.

**No Kafka required** — ``job_producer=None`` so the benchmark measures Postgres
claim throughput only (not broker publish latency).

Prerequisites:
  - Postgres: ``docker compose up -d postgres`` + migrations applied

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/benchmark_scheduler_throughput.py

Examples:

    PYTHONPATH=. python3 control_plane/scripts/benchmark_scheduler_throughput.py --count 200
    PYTHONPATH=. python3 control_plane/scripts/benchmark_scheduler_throughput.py --prefix bench --batch-size 25
"""

from __future__ import annotations

import argparse
import math
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
from control_plane.kernelq.scheduler_tick import SchedulerTickRunner

# High base priority so benchmark rows sort ahead of unrelated local queued jobs.
_BENCHMARK_PRIORITY_BASE = 1_900_000_000


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark SchedulerTickRunner dispatch throughput in Postgres.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="How many queued jobs to generate before dispatching (default: 100).",
    )
    parser.add_argument(
        "--prefix",
        type=str,
        default="sched-bench",
        help='Job id prefix for this run (default: "sched-bench").',
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
        help="Priority spread; jobs cycle 0..max-priority on top of a high base (default: 100).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="max_jobs_per_tick for each scheduler pass (default: 50).",
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
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be a positive integer")


def _tenant_id(index: int, tenant_count: int) -> str:
    """Cycle tenant-0, tenant-1, ... for fairness-style load mixes."""
    return f"tenant-{index % tenant_count}"


def _priority(index: int, max_priority: int) -> int:
    """
    Cycle priorities on a high base so our rows win ordering in a shared DB.

    The base keeps benchmark jobs ahead of typical seed/smoke rows while still
    exercising the priority column the scheduler orders on.
    """
    spread = 1 if max_priority == 0 else (max_priority + 1)
    return _BENCHMARK_PRIORITY_BASE + (index % spread)


def _generate_jobs(
    repo: JobRepository,
    *,
    count: int,
    prefix: str,
    tenants: int,
    max_priority: int,
    run_timestamp: int,
) -> list[str]:
    """Insert queued rows and return the job ids we created."""
    job_ids: list[str] = []

    for index in range(count):
        job_id = f"{prefix}-{run_timestamp}-{index}"
        repo.create_job(
            job_id=job_id,
            tenant_id=_tenant_id(index, tenants),
            priority=_priority(index, max_priority),
            state=JobState.QUEUED.value,
            payload={"kind": "sched-bench", "index": index},
        )
        job_ids.append(job_id)

    return job_ids


def _max_tick_iterations(job_count: int, batch_size: int) -> int:
    """
    Safety cap so a stuck scheduler loop cannot run forever.

    Allows extra ticks when other queued rows compete in a shared database.
    """
    minimum_ticks = math.ceil(job_count / batch_size)
    return max(minimum_ticks * 5, minimum_ticks + 10)


def _run_dispatch_benchmark(
    repo: JobRepository,
    *,
    our_job_ids: set[str],
    batch_size: int,
    max_iterations: int,
) -> tuple[int, int]:
    """
    Run scheduler ticks until done or stuck.

    Returns (dispatched_jobs, tick_count).
    """
    runner = SchedulerTickRunner(
        repo,
        max_jobs_per_tick=batch_size,
        job_producer=None,
    )

    dispatched_our_ids: set[str] = set()
    tick_count = 0

    while len(dispatched_our_ids) < len(our_job_ids) and tick_count < max_iterations:
        progress_before = len(dispatched_our_ids)
        result = runner.run_once()
        tick_count += 1

        if result.errors:
            print("Scheduler tick error:")
            for message in result.errors:
                print(f"  - {message}")
            break

        for job_id in result.dispatched_job_ids:
            if job_id in our_job_ids:
                dispatched_our_ids.add(job_id)

        if len(dispatched_our_ids) == progress_before:
            # No benchmark jobs moved this tick — stop to avoid a busy loop.
            break

    return len(dispatched_our_ids), tick_count


def _print_summary(
    *,
    generated_jobs: int,
    dispatched_jobs: int,
    elapsed_seconds: float,
    jobs_dispatched_per_second: float,
    tick_count: int,
) -> None:
    """Human-readable benchmark output."""
    print("Scheduler throughput benchmark finished.")
    print()
    print(f"  generated_jobs:              {generated_jobs}")
    print(f"  dispatched_jobs:             {dispatched_jobs}")
    print(f"  elapsed_seconds:             {elapsed_seconds}")
    print(f"  jobs_dispatched_per_second:  {jobs_dispatched_per_second}")
    print(f"  tick_count:                  {tick_count}")


def main() -> None:
    args = _parse_args()
    _validate_args(args)

    run_timestamp = int(time.time())
    prefix = args.prefix.strip() or "sched-bench"
    max_iterations = _max_tick_iterations(args.count, args.batch_size)

    # --- Step 1: generate queued jobs ---
    with connect() as conn:
        repo = JobRepository(conn)
        job_ids = _generate_jobs(
            repo,
            count=args.count,
            prefix=prefix,
            tenants=args.tenants,
            max_priority=args.max_priority,
            run_timestamp=run_timestamp,
        )
        our_job_ids = set(job_ids)
        generated_jobs = len(job_ids)

        # --- Step 2: time scheduler ticks only (not job insertion) ---
        started = time.perf_counter()
        dispatched_jobs, tick_count = _run_dispatch_benchmark(
            repo,
            our_job_ids=our_job_ids,
            batch_size=args.batch_size,
            max_iterations=max_iterations,
        )
        elapsed_seconds = time.perf_counter() - started

    jobs_dispatched_per_second = (
        dispatched_jobs / elapsed_seconds if elapsed_seconds > 0 else 0.0
    )

    # --- Step 3: readable summary, then structured log line ---
    _print_summary(
        generated_jobs=generated_jobs,
        dispatched_jobs=dispatched_jobs,
        elapsed_seconds=elapsed_seconds,
        jobs_dispatched_per_second=jobs_dispatched_per_second,
        tick_count=tick_count,
    )
    print()
    print(
        format_log_event(
            "benchmark_scheduler_throughput",
            dispatched_jobs=dispatched_jobs,
            elapsed_seconds=elapsed_seconds,
            generated_jobs=generated_jobs,
            jobs_dispatched_per_second=jobs_dispatched_per_second,
            tick_count=tick_count,
        )
    )


if __name__ == "__main__":
    main()
