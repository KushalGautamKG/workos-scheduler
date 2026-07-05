"""Tests for Kafka result consumer (no real broker required)."""

from __future__ import annotations

import json

import pytest

from control_plane.kernelq.kafka_result_consumer import KafkaResultConsumer
from control_plane.kernelq.result_consumer import ResultConsumerRunner, ResultHandler
from control_plane.kernelq.result_event import RESULT_TOPIC, WORKER_RESULT_EVENT_TYPE


def _valid_payload_bytes(*, job_id: str = "job-123", status: str = "succeeded") -> bytes:
    return json.dumps(
        {
            "event_type": WORKER_RESULT_EVENT_TYPE,
            "job_id": job_id,
            "status": status,
            "message": "ok",
            "worker": "worker-1",
        }
    ).encode()


class FakeKafkaMessage:
    """Minimal stand-in for ``confluent_kafka.Message``."""

    def __init__(
        self,
        *,
        key: bytes | None = b"job-123",
        value: bytes | None = None,
        error: object | None = None,
    ) -> None:
        self._key = key
        self._value = value if value is not None else _valid_payload_bytes()
        self._error = error

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> object | None:
        return self._error


class FakeKafkaConsumer:
    """Records poll/subscribe/close calls for assertions."""

    def __init__(self, *, poll_result: object | None = None) -> None:
        self.poll_result = poll_result
        self.last_poll_timeout: float | None = None
        self.subscribed_topics: list[str] | None = None
        self.closed = False

    def poll(self, timeout: float) -> object | None:
        self.last_poll_timeout = timeout
        return self.poll_result

    def close(self) -> None:
        self.closed = True

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed_topics = topics


class FakeResultHandler(ResultHandler):
    """Records validated events passed from the runner."""

    def __init__(self) -> None:
        self.events: list = []

    def handle(self, event) -> None:
        self.events.append(event)


class RecordingRunner(ResultConsumerRunner):
    """Keeps the last ``ResultMessage`` for key-decode tests."""

    def __init__(self, handler: ResultHandler) -> None:
        super().__init__(handler)
        self.last_message = None

    def process_message(self, message) -> None:
        self.last_message = message
        super().process_message(message)


def _consumer_with_handler() -> tuple[KafkaResultConsumer, FakeKafkaConsumer, FakeResultHandler, RecordingRunner]:
    handler = FakeResultHandler()
    runner = RecordingRunner(handler)
    fake_kafka = FakeKafkaConsumer()
    consumer = KafkaResultConsumer(consumer=fake_kafka, runner=runner)
    return consumer, fake_kafka, handler, runner


def test_process_kafka_message_sends_parsed_event_to_handler():
    consumer, _, handler, _ = _consumer_with_handler()

    consumer.process_kafka_message(
        FakeKafkaMessage(value=_valid_payload_bytes(job_id="job-456"))
    )

    assert len(handler.events) == 1
    assert handler.events[0].job_id == "job-456"
    assert handler.events[0].status == "succeeded"


def test_key_bytes_decode_correctly():
    consumer, _, _, runner = _consumer_with_handler()

    consumer.process_kafka_message(
        FakeKafkaMessage(key=b"job-from-key", value=_valid_payload_bytes(job_id="job-999"))
    )

    assert runner.last_message is not None
    assert runner.last_message.key == "job-from-key"


def test_missing_key_becomes_empty_string():
    consumer, _, _, runner = _consumer_with_handler()

    consumer.process_kafka_message(FakeKafkaMessage(key=None))

    assert runner.last_message is not None
    assert runner.last_message.key == ""


def test_missing_runner_raises_value_error():
    consumer = KafkaResultConsumer(consumer=FakeKafkaConsumer(), runner=None)

    with pytest.raises(ValueError, match="runner"):
        consumer.process_kafka_message(FakeKafkaMessage())


def test_poll_once_returns_false_when_no_message():
    consumer, fake_kafka, handler, _ = _consumer_with_handler()
    fake_kafka.poll_result = None

    assert consumer.poll_once(timeout_seconds=1.5) is False
    assert fake_kafka.last_poll_timeout == 1.5
    assert handler.events == []


def test_poll_once_processes_message_and_returns_true():
    consumer, fake_kafka, handler, _ = _consumer_with_handler()
    fake_kafka.poll_result = FakeKafkaMessage(value=_valid_payload_bytes(job_id="polled-job"))

    assert consumer.poll_once() is True
    assert len(handler.events) == 1
    assert handler.events[0].job_id == "polled-job"


def test_poll_once_raises_runtime_error_on_kafka_error():
    consumer, fake_kafka, handler, _ = _consumer_with_handler()

    class FakeKafkaError:
        def __str__(self) -> str:
            return "broker unavailable"

    fake_kafka.poll_result = FakeKafkaMessage(error=FakeKafkaError())

    with pytest.raises(RuntimeError, match="kafka consumer error"):
        consumer.poll_once()

    assert handler.events == []


def test_close_calls_fake_consumer_close():
    consumer, fake_kafka, _, _ = _consumer_with_handler()

    consumer.close()

    assert fake_kafka.closed is True


def test_injected_fake_consumer_does_not_subscribe():
    """When we inject a consumer, KafkaResultConsumer does not call subscribe."""
    fake_kafka = FakeKafkaConsumer()
    handler = FakeResultHandler()

    KafkaResultConsumer(consumer=fake_kafka, runner=ResultConsumerRunner(handler))

    assert fake_kafka.subscribed_topics is None


def test_kafka_consumer_stats_delegates_to_runner():
    consumer, _, handler, runner = _consumer_with_handler()
    message = FakeKafkaMessage(value=_valid_payload_bytes())

    consumer.process_kafka_message(message)
    consumer.process_kafka_message(message)

    assert len(handler.events) == 1
    assert consumer.stats().duplicate_messages == 1
    assert runner.stats().duplicate_messages == 1


def test_kafka_consumer_stats_zero_without_runner():
    consumer = KafkaResultConsumer(consumer=FakeKafkaConsumer(), runner=None)
    assert consumer.stats().duplicate_messages == 0


def test_real_consumer_init_subscribes_to_result_topic_only(monkeypatch):
    """When building a real client, subscribe only to kernelq.jobs.results."""
    captured: dict = {}

    class CapturingConsumer:
        def __init__(self, config: dict) -> None:
            captured["config"] = config

        def subscribe(self, topics: list[str]) -> None:
            captured["topics"] = topics

        def poll(self, timeout: float) -> None:
            return None

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "control_plane.kernelq.kafka_result_consumer.Consumer",
        CapturingConsumer,
    )

    KafkaResultConsumer()

    assert captured["topics"] == [RESULT_TOPIC]
