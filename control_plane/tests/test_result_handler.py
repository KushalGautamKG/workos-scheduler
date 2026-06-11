"""Tests for mapping worker results to Postgres job state."""

import pytest

from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.result_event import WORKER_RESULT_EVENT_TYPE, WorkerResultEvent
from control_plane.kernelq.result_handler import ResultStateHandler


def _event(*, job_id: str = "job-123", status: str = "succeeded") -> WorkerResultEvent:
    return WorkerResultEvent(
        event_type=WORKER_RESULT_EVENT_TYPE,
        job_id=job_id,
        status=status,
        message="",
        worker="worker-1",
    )


class FakeRepository:
    """Records update calls and returns a configurable success flag."""

    def __init__(self, *, updated: bool = True) -> None:
        self.updated = updated
        self.calls: list[tuple[str, str]] = []

    def update_job_state_from_worker_result(self, job_id: str, new_state: str) -> bool:
        self.calls.append((job_id, new_state))
        return self.updated


def test_succeeded_maps_to_succeeded():
    repo = FakeRepository()
    ResultStateHandler(repo).handle(_event(status="succeeded"))

    assert repo.calls == [("job-123", JobState.SUCCEEDED.value)]


def test_retryable_failure_maps_to_failed_for_now():
    repo = FakeRepository()
    ResultStateHandler(repo).handle(_event(status="retryable_failure"))

    assert repo.calls == [("job-123", JobState.FAILED.value)]


def test_terminal_failure_maps_to_failed_for_now():
    repo = FakeRepository()
    ResultStateHandler(repo).handle(_event(status="terminal_failure"))

    assert repo.calls == [("job-123", JobState.FAILED.value)]


def test_missing_repository_raises():
    with pytest.raises(ValueError, match="repository"):
        ResultStateHandler(None).handle(_event())


def test_unknown_status_raises():
    repo = FakeRepository()
    handler = ResultStateHandler(repo)
    bad = _event(status="unknown")

    with pytest.raises(ValueError, match="unknown result status"):
        handler.handle(bad)

    assert repo.calls == []


def test_repository_false_raises_job_not_found():
    repo = FakeRepository(updated=False)
    handler = ResultStateHandler(repo)

    with pytest.raises(ValueError, match="job not found"):
        handler.handle(_event(job_id="missing-job"))

    assert repo.calls == [("missing-job", JobState.SUCCEEDED.value)]


def test_handler_passes_correct_job_id():
    repo = FakeRepository()
    ResultStateHandler(repo).handle(_event(job_id="job-456"))

    job_id, _state = repo.calls[0]
    assert job_id == "job-456"
