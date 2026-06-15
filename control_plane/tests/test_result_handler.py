"""Tests for mapping worker results to Postgres job state."""

from __future__ import annotations

import pytest

from control_plane.kernelq.job_repository import ScheduleRetryResult
from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.result_event import WORKER_RESULT_EVENT_TYPE, WorkerResultEvent
from control_plane.kernelq.result_handler import ResultStateHandler


def _event(*, job_id: str = "job-123", status: str = "succeeded") -> WorkerResultEvent:
    """Build a small valid worker result event for tests."""
    return WorkerResultEvent(
        event_type=WORKER_RESULT_EVENT_TYPE,
        job_id=job_id,
        status=status,
        message="",
        worker="worker-1",
    )


class FakeRepository:
    """
    Stand-in for JobRepository.

    Records which methods were called so tests can assert routing without Postgres.
    """

    def __init__(
        self,
        *,
        update_ok: bool = True,
        schedule_retry_outcome: str | None = "scheduled",
    ) -> None:
        # update_ok=False simulates a missing job for direct state updates.
        # schedule_retry_outcome:
        #   "scheduled"  -> repository schedules another retry
        #   "exhausted"  -> repository dead-letters the job
        #   None         -> job not found
        self.update_ok = update_ok
        self.schedule_retry_outcome = schedule_retry_outcome
        self.update_calls: list[tuple[str, str]] = []
        self.schedule_retry_calls: list[str] = []
        self.last_schedule_result: ScheduleRetryResult | None = None

    def update_job_state_from_worker_result(self, job_id: str, new_state: str) -> bool:
        self.update_calls.append((job_id, new_state))
        return self.update_ok

    def schedule_retry_from_worker_result(self, job_id: str) -> ScheduleRetryResult | None:
        self.schedule_retry_calls.append(job_id)

        if self.schedule_retry_outcome is None:
            self.last_schedule_result = None
            return None

        if self.schedule_retry_outcome == "exhausted":
            result = ScheduleRetryResult(
                outcome="exhausted",
                job_id=job_id,
                state=JobState.DEAD_LETTERED.value,
                retry_count=3,
                max_retries=3,
                retry_after=1_600_000_000,
            )
        else:
            result = ScheduleRetryResult(
                outcome="scheduled",
                job_id=job_id,
                state=JobState.RETRY_SCHEDULED.value,
                retry_count=1,
                max_retries=3,
                retry_after=1_700_000_000,
            )

        self.last_schedule_result = result
        return result


def test_succeeded_maps_to_succeeded():
    repo = FakeRepository()
    ResultStateHandler(repo).handle(_event(status="succeeded"))

    assert repo.update_calls == [("job-123", JobState.SUCCEEDED.value)]
    assert repo.schedule_retry_calls == []


def test_terminal_failure_maps_to_failed():
    repo = FakeRepository()
    ResultStateHandler(repo).handle(_event(status="terminal_failure"))

    assert repo.update_calls == [("job-123", JobState.FAILED.value)]
    assert repo.schedule_retry_calls == []


def test_retryable_failure_calls_schedule_retry():
    repo = FakeRepository()
    ResultStateHandler(repo).handle(_event(status="retryable_failure"))

    assert repo.schedule_retry_calls == ["job-123"]
    assert repo.update_calls == []


def test_retryable_failure_can_result_in_retry_scheduled():
    repo = FakeRepository(schedule_retry_outcome="scheduled")
    ResultStateHandler(repo).handle(_event(status="retryable_failure"))

    assert repo.last_schedule_result is not None
    assert repo.last_schedule_result.outcome == "scheduled"
    assert repo.last_schedule_result.state == JobState.RETRY_SCHEDULED.value


def test_retryable_failure_can_result_in_dead_lettered_when_exhausted():
    repo = FakeRepository(schedule_retry_outcome="exhausted")
    ResultStateHandler(repo).handle(_event(status="retryable_failure"))

    assert repo.last_schedule_result is not None
    assert repo.last_schedule_result.outcome == "exhausted"
    assert repo.last_schedule_result.state == JobState.DEAD_LETTERED.value
    assert repo.update_calls == []


def test_retryable_failure_missing_job_raises():
    repo = FakeRepository(schedule_retry_outcome=None)
    handler = ResultStateHandler(repo)

    with pytest.raises(ValueError, match="job not found"):
        handler.handle(_event(job_id="missing-job", status="retryable_failure"))

    assert repo.schedule_retry_calls == ["missing-job"]
    assert repo.last_schedule_result is None
    assert repo.update_calls == []


def test_terminal_failure_missing_job_raises():
    repo = FakeRepository(update_ok=False)
    handler = ResultStateHandler(repo)

    with pytest.raises(ValueError, match="job not found"):
        handler.handle(_event(job_id="missing-job", status="terminal_failure"))

    assert repo.update_calls == [("missing-job", JobState.FAILED.value)]
    assert repo.schedule_retry_calls == []


def test_unknown_status_raises():
    repo = FakeRepository()
    handler = ResultStateHandler(repo)

    with pytest.raises(ValueError, match="unknown result status"):
        handler.handle(_event(status="not-a-real-status"))

    assert repo.update_calls == []
    assert repo.schedule_retry_calls == []
