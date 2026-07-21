"""
Kafka dispatch publishing for the Python control plane.

Scheduler ticks (see ``scheduler_tick.py``) will eventually **claim** jobs in
Postgres and then **publish dispatch events** so Go workers can consume runnable
work from Kafka. This module is that publish step only — **not wired into the
tick runner yet**.

Design goals:
- **Beginner-friendly**: small surface area, heavy comments.
- **Testable**: pass a **fake producer** in unit tests (any object with
  ``produce()`` and ``flush()``); only production/local runs use the real
  ``confluent_kafka.Producer``.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any

from confluent_kafka import Producer

# Host clients connect here (matches docker-compose.yml PLAINTEXT_HOST listener).
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"

# Normal runnable-work lane — scheduler publishes here after claiming a job.
DISPATCH_TOPIC = "kernelq.jobs.dispatch"


@dataclass
class DispatchEvent:
    """
    One message on ``kernelq.jobs.dispatch``.

    Workers need enough context to start execution without re-querying Postgres
    for every field (though they should still verify state in the DB).
    """

    event_type: str
    job_id: str
    tenant_id: str
    priority: int
    state: str
    payload: dict

    def to_dict(self) -> dict[str, Any]:
        """Plain dict for logging, tests, or custom serialization."""
        return asdict(self)

    def to_json(self) -> str:
        """JSON string stored as the Kafka message value."""
        return json.dumps(self.to_dict())


class KafkaJobProducer:
    """
    Thin wrapper around a Kafka producer for KernelQ dispatch events.

    **Scheduler integration comes later.** Today this class only knows how to
    publish one ``DispatchEvent`` to ``DISPATCH_TOPIC`` synchronously.
    """

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        producer: Any | None = None,
    ) -> None:
        """
        Build a producer client.

        Parameters
        ----------
        bootstrap_servers:
            Broker address for real clients (default ``localhost:9092``).
        producer:
            Optional injected client. Unit tests pass a **fake** with
            ``produce(topic, key=..., value=...)`` and ``flush()`` instead of
            opening a network connection to Kafka.
        """
        if producer is not None:
            self._producer = producer
        else:
            # Real Confluent client — connects on first produce/flush.
            self._producer = Producer({"bootstrap.servers": bootstrap_servers})

    def publish_dispatch_event(
        self,
        event: DispatchEvent,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        """
        Publish one dispatch event to ``kernelq.jobs.dispatch``.

        - **Key** = ``job_id`` (keeps all events for one job on the same
          partition when partition count > 1).
        - **Value** = JSON from ``event.to_json()``.
        - **headers** = optional Kafka headers (W3C ``traceparent`` when the
          caller injects OpenTelemetry context — Day 122). Payload schema is
          unchanged; tracing never lives inside the JSON body.
        - **flush()** waits until the broker acks (simple, synchronous feel).
        """
        produce_kwargs: dict[str, Any] = {
            "topic": DISPATCH_TOPIC,
            "key": event.job_id,
            "value": event.to_json(),
        }
        if headers:
            produce_kwargs["headers"] = headers
        self._producer.produce(**produce_kwargs)
        # Block until outstanding messages are delivered (or timeout).
        # Keeps the first version easy to reason about; async batching later.
        self._producer.flush()

    def close(self) -> None:
        """Release buffered messages before shutdown (same as a final flush)."""
        self._producer.flush()
