"""
Process worker result messages (in-memory boundary for now).

Go workers publish JSON to ``kernelq.jobs.results``. This module turns raw
message bytes into validated ``WorkerResultEvent`` objects and passes them to
a ``ResultHandler``. Real Kafka subscription comes later; tests use fakes.

**Day 100:** optional idempotency dedupe via ``IdempotencyStore.try_claim`` and
``worker_result_key`` before calling the handler. Duplicates are skipped (not
errors); Postgres updates happen at most once per ``(job_id, attempt)``.
"""

from __future__ import annotations

from dataclasses import dataclass

from .idempotency_keys import worker_result_key
from .idempotency_store import IdempotencyStore, InMemoryIdempotencyStore
from .logging_utils import format_log_event
from .result_event import WorkerResultEvent, parse_worker_result_event

# Default TTL for worker-result dedupe keys (24 hours).
DEFAULT_DEDUPE_TTL_SECONDS = 86400


@dataclass(frozen=True)
class ResultConsumerStats:
    """Counters from ``ResultConsumerRunner`` message processing."""

    duplicate_messages: int = 0


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

    When an ``IdempotencyStore`` is configured (default: in-memory for tests),
    ``process_message`` claims ``worker_result_key(job_id, attempt)`` before
    invoking the handler. A failed claim means this result was already applied —
    skip the handler and count a duplicate.
    """

    def __init__(
        self,
        handler: ResultHandler,
        idempotency_store: IdempotencyStore | None = None,
        dedupe_ttl_seconds: int = DEFAULT_DEDUPE_TTL_SECONDS,
    ) -> None:
        self.handler = handler
        self.idempotency_store = (
            idempotency_store
            if idempotency_store is not None
            else InMemoryIdempotencyStore()
        )
        self.dedupe_ttl_seconds = dedupe_ttl_seconds
        self.duplicate_messages = 0

    def stats(self) -> ResultConsumerStats:
        """Return a snapshot of runner counters (for scripts, tests, metrics)."""
        return ResultConsumerStats(duplicate_messages=self.duplicate_messages)

    def process_message(self, message: ResultMessage) -> None:
        """
        Parse ``message.value``, validate, then call ``handler.handle(event)``.

        Raises ValueError for invalid JSON or invalid event fields.
        Propagates any exception from ``handler.handle``.

        Duplicate results (same ``job_id`` + ``attempt`` while the dedupe key
        is live) skip the handler without raising.
        """
        if self.handler is None:
            raise ValueError("handler must not be None")

        event = parse_worker_result_event(message.value)
        dedupe_key = worker_result_key(event.job_id, event.attempt)

        if not self.idempotency_store.try_claim(
            dedupe_key,
            ttl_seconds=self.dedupe_ttl_seconds,
        ):
            self.duplicate_messages += 1
            print(
                format_log_event(
                    "duplicate_worker_result",
                    attempt=event.attempt,
                    job_id=event.job_id,
                )
            )
            return

        self.handler.handle(event)
