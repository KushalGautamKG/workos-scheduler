"""
Unit tests for Kafka dispatch publishing (no real broker required).

We inject a ``FakeProducer`` instead of ``confluent_kafka.Producer`` so tests
stay fast and do not need ``docker compose up kafka``.
"""

from __future__ import annotations

import json

from control_plane.kernelq.kafka_producer import (
    DISPATCH_TOPIC,
    DispatchEvent,
    KafkaJobProducer,
)


class FakeProducer:
    """
    Stand-in for ``confluent_kafka.Producer``.

    Records the last ``produce()`` call and whether ``flush()`` ran.
    """

    def __init__(self) -> None:
        self.topic: str | None = None
        self.key: str | None = None
        self.value: str | None = None
        self.headers = None
        self.flush_called = False

    def produce(self, topic: str, key: str | None = None, value: str | None = None, **kwargs) -> None:
        self.topic = topic
        self.key = key
        self.value = value
        self.headers = kwargs.get("headers")

    def flush(self) -> None:
        self.flush_called = True


def _sample_event() -> DispatchEvent:
    return DispatchEvent(
        event_type="job.dispatch",
        job_id="job-123",
        tenant_id="tenant-a",
        priority=5,
        state="dispatched",
        payload={"task": "demo"},
    )


def test_dispatch_event_to_dict_returns_expected_fields():
    event = _sample_event()

    result = event.to_dict()

    assert result == {
        "event_type": "job.dispatch",
        "job_id": "job-123",
        "tenant_id": "tenant-a",
        "priority": 5,
        "state": "dispatched",
        "payload": {"task": "demo"},
    }


def test_dispatch_event_to_json_returns_valid_json():
    event = _sample_event()

    raw = event.to_json()
    parsed = json.loads(raw)

    assert parsed["job_id"] == "job-123"
    assert parsed["tenant_id"] == "tenant-a"
    assert parsed["priority"] == 5
    assert parsed["payload"] == {"task": "demo"}


def test_publish_dispatch_event_sends_to_dispatch_topic():
    fake = FakeProducer()
    producer = KafkaJobProducer(producer=fake)
    event = _sample_event()

    producer.publish_dispatch_event(event)

    assert fake.topic == DISPATCH_TOPIC
    assert fake.topic == "kernelq.jobs.dispatch"


def test_publish_uses_job_id_as_kafka_key():
    fake = FakeProducer()
    producer = KafkaJobProducer(producer=fake)
    event = _sample_event()

    producer.publish_dispatch_event(event)

    assert fake.key == "job-123"
    assert fake.key == event.job_id


def test_publish_flushes_producer():
    fake = FakeProducer()
    producer = KafkaJobProducer(producer=fake)

    producer.publish_dispatch_event(_sample_event())

    assert fake.flush_called is True


def test_publish_forwards_optional_headers():
    fake = FakeProducer()
    producer = KafkaJobProducer(producer=fake)
    headers = [("traceparent", b"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")]

    producer.publish_dispatch_event(_sample_event(), headers=headers)

    assert fake.headers == headers


def test_close_flushes_producer():
    fake = FakeProducer()
    producer = KafkaJobProducer(producer=fake)

    producer.close()

    assert fake.flush_called is True
