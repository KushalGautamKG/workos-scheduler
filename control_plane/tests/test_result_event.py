"""Tests for worker result event parsing and validation."""

import json

import pytest

from control_plane.kernelq.result_event import (
    WORKER_RESULT_EVENT_TYPE,
    WorkerResultEvent,
    parse_worker_result_event,
)


def _valid_json(
    *,
    job_id: str = "job-123",
    status: str = "succeeded",
    message: str = "ok",
    worker: str = "worker-1",
    event_type: str = WORKER_RESULT_EVENT_TYPE,
) -> str:
    """Build a minimal valid result event JSON string for tests."""
    return json.dumps(
        {
            "event_type": event_type,
            "job_id": job_id,
            "status": status,
            "message": message,
            "worker": worker,
        }
    )


def test_parse_valid_result_event_succeeds():
    raw = _valid_json()
    event = parse_worker_result_event(raw)

    assert event.event_type == WORKER_RESULT_EVENT_TYPE
    assert event.job_id == "job-123"
    assert event.status == "succeeded"
    assert event.message == "ok"
    assert event.worker == "worker-1"


@pytest.mark.parametrize(
    "status",
    ["succeeded", "retryable_failure", "terminal_failure"],
)
def test_valid_statuses_are_accepted(status: str):
    event = parse_worker_result_event(_valid_json(status=status))
    assert event.status == status


def test_wrong_event_type_fails():
    with pytest.raises(ValueError, match="event_type"):
        parse_worker_result_event(_valid_json(event_type="job.dispatch"))


def test_blank_job_id_fails():
    with pytest.raises(ValueError, match="job_id"):
        parse_worker_result_event(_valid_json(job_id="   "))


def test_invalid_status_fails():
    with pytest.raises(ValueError, match="status"):
        parse_worker_result_event(_valid_json(status="unknown"))


def test_blank_worker_fails():
    with pytest.raises(ValueError, match="worker"):
        parse_worker_result_event(_valid_json(worker=""))


def test_malformed_json_fails():
    with pytest.raises(ValueError, match="invalid JSON"):
        parse_worker_result_event("{not valid json")


def test_to_json_returns_expected_fields():
    event = WorkerResultEvent(
        event_type=WORKER_RESULT_EVENT_TYPE,
        job_id="job-456",
        status="retryable_failure",
        message="timeout",
        worker="worker-2",
    )
    parsed = json.loads(event.to_json())

    assert parsed == {
        "event_type": WORKER_RESULT_EVENT_TYPE,
        "job_id": "job-456",
        "status": "retryable_failure",
        "message": "timeout",
        "worker": "worker-2",
        "attempt": 0,
    }


def test_message_may_be_blank():
    event = parse_worker_result_event(_valid_json(message=""))
    assert event.message == ""

    direct = WorkerResultEvent(
        event_type=WORKER_RESULT_EVENT_TYPE,
        job_id="job-789",
        status="terminal_failure",
        message="",
        worker="worker-3",
    )
    direct.validate()  # should not raise
