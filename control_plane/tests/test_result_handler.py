"""Tests for mapping worker results to Postgres job state."""

import pytest

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

    def __init__(self, *, update_ok: bool = True, schedule_retry_ok: bool = True) -> None:
        # When False, the matching method pretends the job row was missing.
        self.update_ok = update_ok
        self.schedule_retry_ok = schedule_retry_ok
        # Every (job_id, new_state) passed to update_job_state_from_worker_result.
        self.update_calls: list[tuple[str, str]] = []
        # Every job_id passed to schedule_retry_from_worker_result.
        self.schedule_retry_calls: list[str] = []

    def update_job_state_from_worker_result(self, job_id: str, new_state: str) -> bool:
        self.update_calls.append((job_id, new_state))
        return self.update_ok

    def schedule_retry_from_worker_result(self, job_id: str) -> bool:
        self.schedule_retry_calls.append(job_id)
        return self.schedule_retry_ok


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


def test_retryable_failure_does_not_call_update_state():
    repo = FakeRepository()
    ResultStateHandler(repo).handle(_event(status="retryable_failure"))

    assert repo.update_calls == []


def test_retryable_failure_missing_job_raises():
    repo = FakeRepository(schedule_retry_ok=False)
    handler = ResultStateHandler(repo)

    with pytest.raises(ValueError, match="job not found"):
        handler.handle(_event(job_id="missing-job", status="retryable_failure"))

    assert repo.schedule_retry_calls == ["missing-job"]
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
