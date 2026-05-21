"""
One scheduler tick: atomically claim schedulable jobs from Postgres.

This module is intentionally small and synchronous:
- No Kafka publishing yet (that comes in a later milestone).
- No async or threading yet (one tick runs to completion on the caller's thread).

A *tick* is one pass of the dispatch loop in the Python control plane:
ask Postgres to lock and claim up to N ``queued`` jobs as ``dispatched``,
then return a summary of what happened.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from control_plane.kernelq.job_repository import JobRepository


@dataclass
class SchedulerTickResult:
    """
    Summary of a single scheduler tick.

    - selected_count: how many jobs were claimed this tick
    - dispatched_count: how many were successfully marked ``dispatched``
    - dispatched_job_ids: ids that moved to ``dispatched`` (in scheduling order)
    - skipped_count: selected jobs that were not dispatched and did not error
      (for example another process already claimed the row)
    - errors: human-readable problem descriptions when the repository call fails
    """

    selected_count: int
    dispatched_count: int
    dispatched_job_ids: list[str] = field(default_factory=list)
    skipped_count: int = 0
    errors: list[str] = field(default_factory=list)


class SchedulerTickRunner:
    """
    Run one database-backed scheduler tick using ``JobRepository``.

    Typical use (later, from a loop or cron in the control plane):

        runner = SchedulerTickRunner(repository, max_jobs_per_tick=10)
        result = runner.run_once()
        # inspect result.dispatched_count, result.errors, etc.

    ``max_jobs_per_tick`` caps how many rows we claim per pass so one tick cannot
    overload downstream systems once Kafka and workers are connected.
    """

    def __init__(self, repository: JobRepository, max_jobs_per_tick: int = 10) -> None:
        if max_jobs_per_tick <= 0:
            raise ValueError("max_jobs_per_tick must be a positive integer")

        self._repository = repository
        self._max_jobs_per_tick = max_jobs_per_tick

    def run_once(self) -> SchedulerTickResult:
        """
        Execute one scheduler tick.

        Calls ``claim_schedulable_jobs`` once. That method uses a single Postgres
        transaction with ``FOR UPDATE SKIP LOCKED`` so multiple scheduler
        instances do not claim the same row (no duplicate dispatch).
        """
        try:
            claimed_jobs = self._repository.claim_schedulable_jobs(
                limit=self._max_jobs_per_tick
            )
        except Exception as exc:
            return SchedulerTickResult(
                selected_count=0,
                dispatched_count=0,
                dispatched_job_ids=[],
                skipped_count=0,
                errors=[f"claim_schedulable_jobs: {type(exc).__name__}: {exc}"],
            )

        dispatched_job_ids = [job.job_id for job in claimed_jobs]
        claimed_count = len(claimed_jobs)

        return SchedulerTickResult(
            selected_count=claimed_count,
            dispatched_count=claimed_count,
            dispatched_job_ids=dispatched_job_ids,
            skipped_count=0,
            errors=[],
        )
