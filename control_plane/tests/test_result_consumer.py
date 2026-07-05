"""Tests for the in-memory result consumer runner."""

import json

import pytest

from control_plane.kernelq.idempotency_keys import worker_result_key
from control_plane.kernelq.idempotency_store import IdempotencyStore
from control_plane.kernelq.result_consumer import (
    DEFAULT_DEDUPE_TTL_SECONDS,
    ResultConsumerRunner,
    ResultHandler,
    ResultMessage,
)
from control_plane.kernelq.result_event import WORKER_RESULT_EVENT_TYPE


def _valid_payload_bytes(
    *,
    job_id: str = "job-123",
    status: str = "succeeded",
    attempt: int = 0,
) -> bytes:
    return json.dumps(
        {
            "event_type": WORKER_RESULT_EVENT_TYPE,
            "job_id": job_id,
            "status": status,
            "message": "ok",
            "worker": "worker-1",
            "attempt": attempt,
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


class RecordingIdempotencyStore(IdempotencyStore):
    """Fake store that records claim calls (no Redis)."""

    def __init__(self) -> None:
        self.claims: list[tuple[str, int]] = []
        self._live_keys: set[str] = set()

    def try_claim(self, key: str, ttl_seconds: int) -> bool:
        self.claims.append((key, ttl_seconds))
        if key in self._live_keys:
            return False
        self._live_keys.add(key)
        return True


class FailingIdempotencyStore(IdempotencyStore):
    """Raises on every claim to simulate store outage."""

    def try_claim(self, key: str, ttl_seconds: int) -> bool:
        raise RuntimeError("idempotency store unavailable")


def test_first_result_calls_handler():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)

    runner.process_message(ResultMessage(key="job-123", value=_valid_payload_bytes()))

    assert len(handler.events) == 1
    assert handler.events[0].job_id == "job-123"


def test_duplicate_result_skips_handler():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)
    message = ResultMessage(key="job-123", value=_valid_payload_bytes())

    runner.process_message(message)
    runner.process_message(message)

    assert len(handler.events) == 1


def test_duplicate_messages_increments():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)
    message = ResultMessage(key="job-123", value=_valid_payload_bytes())

    runner.process_message(message)
    runner.process_message(message)
    runner.process_message(message)

    assert runner.duplicate_messages == 2
    assert runner.stats().duplicate_messages == 2


def test_duplicate_log_emitted(capsys):
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)
    message = ResultMessage(
        key="job-123",
        value=_valid_payload_bytes(job_id="job-123", attempt=2),
    )

    runner.process_message(message)
    runner.process_message(message)

    captured = capsys.readouterr()
    assert "event=duplicate_worker_result" in captured.out
    assert "attempt=2" in captured.out
    assert "job_id=job-123" in captured.out


def test_same_job_different_attempt_processes():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)

    runner.process_message(
        ResultMessage(key="job-123", value=_valid_payload_bytes(attempt=0))
    )
    runner.process_message(
        ResultMessage(key="job-123", value=_valid_payload_bytes(attempt=1))
    )

    assert len(handler.events) == 2
    assert handler.events[0].attempt == 0
    assert handler.events[1].attempt == 1
    assert runner.duplicate_messages == 0


def test_different_job_same_attempt_processes():
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler)

    runner.process_message(
        ResultMessage(key="job-a", value=_valid_payload_bytes(job_id="job-a", attempt=0))
    )
    runner.process_message(
        ResultMessage(key="job-b", value=_valid_payload_bytes(job_id="job-b", attempt=0))
    )

    assert len(handler.events) == 2
    assert {event.job_id for event in handler.events} == {"job-a", "job-b"}
    assert runner.duplicate_messages == 0


def test_custom_idempotency_store_is_used():
    handler = FakeResultHandler()
    store = RecordingIdempotencyStore()
    runner = ResultConsumerRunner(handler, idempotency_store=store)

    runner.process_message(
        ResultMessage(key="job-123", value=_valid_payload_bytes(job_id="job-123", attempt=0))
    )

    assert store.claims == [
        (worker_result_key("job-123", 0), DEFAULT_DEDUPE_TTL_SECONDS),
    ]


def test_idempotency_store_failure_propagates():
    """
    Store errors propagate (fail fast).

    ``ResultConsumerRunner`` does not swallow ``try_claim`` exceptions — a
    down Redis (or other backend) surfaces to the caller so scripts can count
    ``errors_count=1`` instead of silently skipping or double-applying results.
    """
    handler = FakeResultHandler()
    runner = ResultConsumerRunner(handler, idempotency_store=FailingIdempotencyStore())

    with pytest.raises(RuntimeError, match="idempotency store unavailable"):
        runner.process_message(ResultMessage(key="job-123", value=_valid_payload_bytes()))

    assert handler.events == []
    assert runner.duplicate_messages == 0


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


def test_stats_starts_at_zero():
    runner = ResultConsumerRunner(FakeResultHandler())
    assert runner.stats().duplicate_messages == 0
