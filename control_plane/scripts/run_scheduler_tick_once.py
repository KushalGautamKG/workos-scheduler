#!/usr/bin/env python3
"""
Run one real scheduler tick: claim queued jobs in Postgres, publish to Kafka.

This is a **manual integration script** for local development. It does **not**
create jobs—you must already have rows in state ``queued`` (for example via the
HTTP API or SQL).

Prerequisites (all must be running before you run this script):
  - Postgres: ``docker compose up -d postgres`` + migrations applied
  - Kafka: ``docker compose up -d zookeeper kafka``
  - Topics: ``./infra/kafka/create-topics.sh``

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/run_scheduler_tick_once.py

Or:

    python3 control_plane/scripts/run_scheduler_tick_once.py

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
from control_plane.kernelq.kafka_producer import (
    DEFAULT_BOOTSTRAP_SERVERS,
    KafkaJobProducer,
)
from control_plane.kernelq.logging_utils import format_log_event
from control_plane.kernelq.scheduler_tick import SchedulerTickRunner


def _print_summary(result) -> None:
    """Print tick outcome in a readable, copy-paste-friendly format."""
    print("Scheduler tick finished.")
    print()
    print(f"  selected_count:    {result.selected_count}")
    print(f"  dispatched_count:  {result.dispatched_count}")
    print(f"  published_count:   {result.published_count}")
    print()
    print("  dispatched_job_ids:")
    if result.dispatched_job_ids:
        for job_id in result.dispatched_job_ids:
            print(f"    - {job_id}")
    else:
        print("    (none — no queued jobs were claimed this tick)")
    print()
    print("  errors:")
    if result.errors:
        for message in result.errors:
            print(f"    - {message}")
    else:
        print("    (none)")
    print()
    print("  publish_errors:")
    if result.publish_errors:
        for message in result.publish_errors:
            print(f"    - {message}")
    else:
        print("    (none)")


def _print_structured_summary(result) -> None:
    """Print one grep-friendly line for log collectors and scripts."""
    print(
        format_log_event(
            "scheduler_tick",
            selected_count=result.selected_count,
            dispatched_count=result.dispatched_count,
            published_count=result.published_count,
            errors_count=len(result.errors),
            publish_errors_count=len(result.publish_errors),
        )
    )


def main() -> None:
    # --- Step 1: Postgres connection and repository ---
    # JobRepository runs claim_schedulable_jobs inside this connection.
    with connect() as conn:
        repository = JobRepository(conn)

        # --- Step 2: Real Kafka producer (host listener on localhost:9092) ---
        job_producer = KafkaJobProducer(bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS)

        try:
            # --- Step 3: One tick, claim at most one job, publish after claim ---
            runner = SchedulerTickRunner(
                repository,
                max_jobs_per_tick=1,
                job_producer=job_producer,
            )

            # --- Step 4: Synchronous single pass ---
            result = runner.run_once()

            # --- Step 5: Human-readable summary, then one structured log line ---
            _print_summary(result)
            _print_structured_summary(result)
        finally:
            # Flush any buffered Kafka messages before exit.
            job_producer.close()


if __name__ == "__main__":
    main()
