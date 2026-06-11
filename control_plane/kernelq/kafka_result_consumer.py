"""
Kafka consumer for worker result events (single-message path for now).

Go workers publish to ``kernelq.jobs.results``. This module reads **one record
at a time** from Kafka and hands bytes to ``ResultConsumerRunner`` — no
infinite loop yet; a long-running daemon comes later.

Tests inject a **fake consumer** (any object with ``poll()`` and ``close()``)
so pytest does not need a broker.
"""

from __future__ import annotations

from confluent_kafka import Consumer

from control_plane.kernelq.result_consumer import ResultConsumerRunner, ResultMessage
from control_plane.kernelq.result_event import RESULT_TOPIC

# Host clients connect here (matches docker-compose.yml PLAINTEXT_HOST listener).
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"

# Consumer group for the Python control plane reading result events.
DEFAULT_GROUP_ID = "kernelq-control-plane-results"


class KafkaResultConsumer:
    """
    Thin Kafka transport layer for ``kernelq.jobs.results``.

    Parsing, validation, and Postgres updates stay in ``ResultConsumerRunner``
    and ``ResultHandler`` — this class only polls and builds ``ResultMessage``.
    """

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        group_id: str = DEFAULT_GROUP_ID,
        consumer: object | None = None,
        runner: ResultConsumerRunner | None = None,
    ) -> None:
        self._runner = runner

        if consumer is not None:
            self._consumer = consumer
        else:
            self._consumer = Consumer(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "group.id": group_id,
                    "auto.offset.reset": "earliest",
                }
            )
            self._consumer.subscribe([RESULT_TOPIC])

    def process_kafka_message(self, message) -> None:
        """
        Turn one ``confluent_kafka.Message`` into a ``ResultMessage`` and process it.

        Raises ValueError if ``runner`` was not configured.
        """
        if self._runner is None:
            raise ValueError("runner must be set before processing messages")

        raw_key = message.key()
        key = raw_key.decode("utf-8") if raw_key is not None else ""
        value = message.value()

        result_message = ResultMessage(key=key, value=value)
        self._runner.process_message(result_message)

    def poll_once(self, timeout_seconds: float = 5.0) -> bool:
        """
        Poll the broker once.

        Returns True if a message was received and processed, False if the poll
        timed out with no message. Raises RuntimeError on Kafka consumer errors.
        """
        message = self._consumer.poll(timeout_seconds)

        if message is None:
            return False

        if message.error():
            raise RuntimeError(f"kafka consumer error: {message.error()}")

        self.process_kafka_message(message)
        return True

    def close(self) -> None:
        """Release broker resources."""
        self._consumer.close()
