"""
Apply worker result events to durable job state in Postgres.

``ResultStateHandler`` is the first real ``ResultHandler`` implementation:
it maps ``WorkerResultEvent.status`` to a ``jobs.state`` value and writes
through ``JobRepository``. Retry scheduling (``RETRY_SCHEDULED``,
``DEAD_LETTERED``) will be added later.
"""

from __future__ import annotations

from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.result_consumer import ResultHandler
from control_plane.kernelq.result_event import WorkerResultEvent


class ResultStateHandler(ResultHandler):
    """
    Update Postgres when a validated worker result event arrives.

    Today this is a simple status → state mapping with no retry policy.
    """

    def __init__(self, repository) -> None:
        self.repository = repository

    def handle(self, event: WorkerResultEvent) -> None:
        if self.repository is None:
            raise ValueError("repository must not be None")

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

    Retry scheduling (``retryable_failure`` → ``RETRY_SCHEDULED`` when retries
    remain, ``terminal_failure`` → ``DEAD_LETTERED``) is intentionally deferred.
    """
    if status == "succeeded":
        return JobState.SUCCEEDED.value
    if status == "retryable_failure":
        return JobState.FAILED.value
    if status == "terminal_failure":
        return JobState.FAILED.value

    raise ValueError(f"unknown result status: {status!r}")
