#!/usr/bin/env python3
"""
Poll Kafka once for a worker result event and update Postgres job state.

This is a **manual integration script** for local development. It reads **at
most one** message from ``kernelq.jobs.results``, validates it, and applies
``ResultStateHandler`` so ``jobs.state`` reflects the worker outcome.

Prerequisites (all must be running before you run this script):
  - Postgres: ``docker compose up -d postgres`` + migrations applied
  - Kafka: ``docker compose up -d zookeeper kafka``
  - Topics: ``./infra/kafka/create-topics.sh``
  - At least one ``WorkerResultEvent`` on ``kernelq.jobs.results`` (for example
    from ``./worker/scripts/smoke_worker_result.sh``)
  - Matching ``job_id`` row in Postgres if you expect a state update

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/consume_result_once.py

Or:

    python3 control_plane/scripts/consume_result_once.py

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
from control_plane.kernelq.kafka_result_consumer import KafkaResultConsumer
from control_plane.kernelq.logging_utils import format_log_event
from control_plane.kernelq.result_consumer import ResultConsumerRunner
from control_plane.kernelq.result_handler import ResultStateHandler

# How long to wait for one result message from Kafka.
POLL_TIMEOUT_SECONDS = 10.0


def _print_structured_summary(
    *,
    processed_message: bool,
    errors_count: int,
    error: str | None = None,
) -> None:
    """Print one grep-friendly line for log collectors and scripts."""
    fields: dict[str, object] = {
        "processed_message": processed_message,
        "errors_count": errors_count,
    }
    if error is not None:
        fields["error"] = error

    print(format_log_event("result_consumer", **fields))


def main() -> None:
    kafka_consumer: KafkaResultConsumer | None = None

    try:
        # --- Step 1: Postgres connection and repository ---
        with connect() as conn:
            repository = JobRepository(conn)

            # --- Step 2: Result pipeline (parse → map status → update jobs.state) ---
            handler = ResultStateHandler(repository)
            runner = ResultConsumerRunner(handler)

            # --- Step 3: Real Kafka consumer on kernelq.jobs.results ---
            kafka_consumer = KafkaResultConsumer(runner=runner)

            # --- Step 4: Poll once (no infinite loop yet) ---
            processed = kafka_consumer.poll_once(timeout_seconds=POLL_TIMEOUT_SECONDS)

            # Human-readable line, then structured summary (errors_count=0 on success).
            print(f"poll_result: processed_message={str(processed).lower()}")
            _print_structured_summary(
                processed_message=processed,
                errors_count=0,
            )
    except Exception as exc:
        # Log failure in the same format, then let the exception propagate.
        _print_structured_summary(
            processed_message=False,
            errors_count=1,
            error=str(exc),
        )
        raise
    finally:
        if kafka_consumer is not None:
            kafka_consumer.close()


if __name__ == "__main__":
    main()
