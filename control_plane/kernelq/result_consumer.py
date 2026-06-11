"""
Process worker result messages (in-memory boundary for now).

Go workers publish JSON to ``kernelq.jobs.results``. This module turns raw
message bytes into validated ``WorkerResultEvent`` objects and passes them to
a ``ResultHandler``. Real Kafka subscription comes later; tests use fakes.
"""

from __future__ import annotations

from dataclasses import dataclass

from .result_event import WorkerResultEvent, parse_worker_result_event


@dataclass(frozen=True)
class ResultMessage:
    """
    Minimal stand-in for a Kafka record on ``kernelq.jobs.results``.

    - ``key``: usually the job_id (Kafka message key from the worker).
    - ``value``: raw JSON bytes for the worker result event.
    """

    key: str
    value: bytes


class ResultHandler:
    """
    Application hook: what to do with a validated result event.

    Subclass this (or use a test fake) to update Postgres, emit metrics, etc.
    """

    def handle(self, event: WorkerResultEvent) -> None:
        raise NotImplementedError


class ResultConsumerRunner:
    """
    Parse one result message and delegate to a handler.

    This is the Python-side mirror of the Go worker's ``ConsumerRunner`` pattern:
    transport (Kafka) stays outside; this class only parses + dispatches.
    """

    def __init__(self, handler: ResultHandler) -> None:
        self.handler = handler

    def process_message(self, message: ResultMessage) -> None:
        """
        Parse ``message.value``, validate, then call ``handler.handle(event)``.

        Raises ValueError for invalid JSON or invalid event fields.
        Propagates any exception from ``handler.handle``.
        """
        if self.handler is None:
            raise ValueError("handler must not be None")

        event = parse_worker_result_event(message.value)
        self.handler.handle(event)
