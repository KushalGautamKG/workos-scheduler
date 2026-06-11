"""Tests for the in-memory result consumer runner."""

import json

import pytest

from control_plane.kernelq.result_consumer import (
    ResultConsumerRunner,
    ResultHandler,
    ResultMessage,
)
from control_plane.kernelq.result_event import WORKER_RESULT_EVENT_TYPE


def _valid_payload_bytes(
    *,
    job_id: str = "job-123",
    status: str = "succeeded",
) -> bytes:
    return json.dumps(
        {
            "event_type": WORKER_RESULT_EVENT_TYPE,
            "job_id": job_id,
            "status": status,
            "message": "ok",
            "worker": "worker-1",
        }
    ).encode()


class FakeResultHandler(ResultHandler):
    """Records events passed to handle() for assertions."""

    def __init__(self) -> None:
        self.events: list = []
        self.error: Exception | None = None

    def handle(self, event) -> None:
        if self.error is not None:
            raise self.error
        self.events.append(event)


def test_process_message_calls_handler_for_valid_json():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)

    runner.process_message(ResultMessage(key="job-123", value=_valid_payload_bytes()))

    assert len(handler.events) == 1


def test_handler_receives_correct_job_id_and_status():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)

    runner.process_message(
        ResultMessage(
            key="job-456",
            value=_valid_payload_bytes(job_id="job-456", status="retryable_failure"),
        )
    )

    event = handler.events[0]
    assert event.job_id == "job-456"
    assert event.status == "retryable_failure"


def test_malformed_json_raises_and_handler_not_called():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)

    with pytest.raises(ValueError, match="invalid JSON"):
        runner.process_message(ResultMessage(key="k", value=b"{not json"))

    assert handler.events == []


def test_invalid_result_event_raises_and_handler_not_called():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)
    bad = json.dumps(
        {
            "event_type": WORKER_RESULT_EVENT_TYPE,
            "job_id": "job-1",
            "status": "unknown",
            "message": "",
            "worker": "worker-1",
        }
    ).encode()

    with pytest.raises(ValueError, match="status"):
        runner.process_message(ResultMessage(key="job-1", value=bad))

    assert handler.events == []


def test_handler_error_propagates():
    handler = FakeResultHandler()
    handler.error = RuntimeError("postgres down")
    runner = ResultConsumerRunner(handler)

    with pytest.raises(RuntimeError, match="postgres down"):
        runner.process_message(ResultMessage(key="job-123", value=_valid_payload_bytes()))


def test_missing_handler_raises_value_error():
    runner = ResultConsumerRunner(None)

    with pytest.raises(ValueError, match="handler"):
        runner.process_message(ResultMessage(key="k", value=_valid_payload_bytes()))
