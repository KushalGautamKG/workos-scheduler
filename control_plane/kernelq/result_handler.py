"""
Apply worker result events to durable job state in Postgres.

``ResultStateHandler`` maps ``WorkerResultEvent.status`` to lifecycle updates
through ``JobRepository``. For ``retryable_failure``, the **repository** applies
retry policy (``RETRY_SCHEDULED`` vs ``DEAD_LETTERED`` when retries are exhausted).
"""

from __future__ import annotations

from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.result_consumer import ResultHandler
from control_plane.kernelq.result_event import WorkerResultEvent


class ResultStateHandler(ResultHandler):
    """
    Update Postgres when a validated worker result event arrives.

    - ``succeeded`` → ``SUCCEEDED`` via a direct state update.
    - ``retryable_failure`` → ``schedule_retry_from_worker_result`` (repository
      chooses ``RETRY_SCHEDULED`` or ``DEAD_LETTERED``).
    - ``terminal_failure`` → ``FAILED`` for now (``DEAD_LETTERED`` later).
    """

    def __init__(self, repository) -> None:
        self.repository = repository

    def handle(self, event: WorkerResultEvent) -> None:
        if self.repository is None:
            raise ValueError("repository must not be None")

        if event.status == "retryable_failure":
            # Worker reported a transient failure. The repository owns retry
            # policy: schedule another attempt (RETRY_SCHEDULED) or stop when
            # max_retries is exhausted (DEAD_LETTERED).
            result = self.repository.schedule_retry_from_worker_result(event.job_id)
            if result is None:
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

    ``retryable_failure`` is **not** mapped here — see ``handle`` and
    ``schedule_retry_from_worker_result``.
    """
    if status == "succeeded":
        return JobState.SUCCEEDED.value
    if status == "terminal_failure":
        return JobState.FAILED.value

    raise ValueError(f"unknown result status: {status!r}")
