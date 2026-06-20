"""
Job duration metrics derived from stored job timestamps.

Computes simple averages for completed jobs (terminal states). Jobs missing
required timestamps for a given metric are skipped for that average only.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

# States treated as "completed" for duration metrics (per product spec).
_COMPLETED_STATES = frozenset({"succeeded", "failed", "dead_lettered"})


@dataclass
class JobDurationMetrics:
    """Aggregate duration stats over a batch of completed jobs."""

    completed_jobs_count: int
    average_queue_wait_seconds: float
    average_completion_seconds: float


def _to_epoch_seconds(value: object) -> float | None:
    """Convert a stored timestamp to Unix seconds, or None if unavailable."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return None


def compute_job_duration_metrics(jobs: Iterable[Any]) -> JobDurationMetrics:
    """
    Compute average queue wait and completion time for terminal-state jobs.

    Each job is any object with ``state``, ``created_at``, and optionally
    ``updated_at`` / ``dispatched_at``. Jobs not in a completed terminal state
    are ignored. Jobs missing timestamps needed for a metric are skipped for
    that average only.

    Queue wait: ``dispatched_at - created_at`` (jobs without ``dispatched_at`` or
    with negative wait are skipped for that average only).
    Completion: ``updated_at - created_at``
    """
    completed_count = 0
    queue_wait_total = 0.0
    queue_wait_count = 0
    completion_total = 0.0
    completion_count = 0

    for job in jobs:
        state = getattr(job, "state", None)
        if state not in _COMPLETED_STATES:
            continue

        completed_count += 1

        created_at = _to_epoch_seconds(getattr(job, "created_at", None))
        updated_at = _to_epoch_seconds(getattr(job, "updated_at", None))
        dispatched_at = _to_epoch_seconds(getattr(job, "dispatched_at", None))

        if created_at is not None and dispatched_at is not None:
            queue_wait = dispatched_at - created_at
            if queue_wait >= 0:
                queue_wait_total += queue_wait
                queue_wait_count += 1

        if created_at is not None and updated_at is not None:
            completion_total += updated_at - created_at
            completion_count += 1

    average_queue_wait = (
        queue_wait_total / queue_wait_count if queue_wait_count else 0.0
    )
    average_completion = (
        completion_total / completion_count if completion_count else 0.0
    )

    return JobDurationMetrics(
        completed_jobs_count=completed_count,
        average_queue_wait_seconds=float(average_queue_wait),
        average_completion_seconds=float(average_completion),
    )
