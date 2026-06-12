"""
Apply worker result events to durable job state in Postgres.

``ResultStateHandler`` maps ``WorkerResultEvent.status`` to lifecycle updates
through ``JobRepository``. ``retryable_failure`` uses retry scheduling;
``terminal_failure`` and requeue/backoff for exhausted retries come later.
"""

from __future__ import annotations

from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.result_consumer import ResultHandler
from control_plane.kernelq.result_event import WorkerResultEvent


class ResultStateHandler(ResultHandler):
    """
    Update Postgres when a validated worker result event arrives.

    ``succeeded`` and ``terminal_failure`` map directly to job state.
    ``retryable_failure`` delegates to ``schedule_retry_from_worker_result``.
    """

    def __init__(self, repository) -> None:
        self.repository = repository

    def handle(self, event: WorkerResultEvent) -> None:
        if self.repository is None:
            raise ValueError("repository must not be None")

        if event.status == "retryable_failure":
            # retryable_failure now moves through retry scheduling logic;
            # actual requeue/backoff comes later.
            scheduled = self.repository.schedule_retry_from_worker_result(event.job_id)
            if not scheduled:
                raise ValueError(f"job not found: {event.job_id!r}")
            return

        new_state = _map_result_status_to_job_state(event.status)

        updated = self.repository.update_job_state_from_worker_result(
            event.job_id,
            new_state,
        )
        if not updated:
            raise ValueError(f"job not found: {event.job_id!r}")


def _map_result_status_to_job_state(status: str) -> str:
    """
    Map worker execution status to a Postgres ``jobs.state`` string.

    ``retryable_failure`` is handled in ``ResultStateHandler.handle`` via
    ``schedule_retry_from_worker_result`` — not this helper.
    """
    if status == "succeeded":
        return JobState.SUCCEEDED.value
    if status == "terminal_failure":
        return JobState.FAILED.value

    raise ValueError(f"unknown result status: {status!r}")
