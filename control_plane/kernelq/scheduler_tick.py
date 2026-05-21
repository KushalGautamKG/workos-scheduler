"""
One scheduler tick: pick schedulable jobs from Postgres and mark them dispatched.

This module is intentionally small and synchronous:
- No Kafka publishing yet (that comes in a later milestone).
- No async or threading yet (one tick runs to completion on the caller's thread).

A *tick* is one pass of the dispatch loop in the Python control plane:
ask the database which jobs are waiting, try to claim each one as ``dispatched``,
and return a summary of what happened.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from control_plane.kernelq.job_repository import JobRepository


@dataclass
class SchedulerTickResult:
    """
    Summary of a single scheduler tick.

    - selected_count: how many jobs ``list_schedulable_jobs`` returned this tick
    - dispatched_count: how many were successfully marked ``dispatched``
    - dispatched_job_ids: ids that moved to ``dispatched`` (in processing order)
    - skipped_count: selected jobs that were not dispatched and did not error
      (for example another process already claimed the row)
    - errors: human-readable problem descriptions for jobs that raised exceptions
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

    ``max_jobs_per_tick`` caps how many rows we try per pass so one tick cannot
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

        Steps:
        1. Load up to ``max_jobs_per_tick`` schedulable rows (``queued``, ordered
           by priority then age).
        2. For each row, try ``mark_job_dispatched`` (``queued`` -> ``dispatched``).
        3. Return counts and any per-job errors (other jobs still run).
        """
        dispatched_job_ids: list[str] = []
        errors: list[str] = []
        dispatched_count = 0

        # Step 1: ask Postgres which jobs are ready to leave the waiting line.
        jobs = self._repository.list_schedulable_jobs(limit=self._max_jobs_per_tick)
        selected_count = len(jobs)

        # Step 2: try to claim each selected job. Failures on one job do not stop the rest.
        for job in jobs:
            try:
                updated = self._repository.mark_job_dispatched(job.job_id)
            except Exception as exc:
                # Record the problem and continue with the next job.
                errors.append(f"job_id={job.job_id}: {type(exc).__name__}: {exc}")
                continue

            if updated is not None:
                dispatched_count += 1
                dispatched_job_ids.append(job.job_id)
            # If ``mark_job_dispatched`` returns None, the row was not claimed
            # (missing job, or no longer ``queued``). That counts as skipped below.

        # Step 3: skipped = selected but neither dispatched nor failed with an exception.
        skipped_count = selected_count - dispatched_count - len(errors)

        return SchedulerTickResult(
            selected_count=selected_count,
            dispatched_count=dispatched_count,
            dispatched_job_ids=dispatched_job_ids,
            skipped_count=skipped_count,
            errors=errors,
        )
