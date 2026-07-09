"""
One scheduler tick: atomically claim schedulable jobs from Postgres.

This module is intentionally small and synchronous:
- Optional Kafka publishing when a ``job_producer`` is injected.
- No async or threading yet (one tick runs to completion on the caller's thread).

A *tick* is one pass of the dispatch loop in the Python control plane:
ask Postgres to lock and claim up to N ``queued`` jobs as ``dispatched``,
optionally publish dispatch events to Kafka, then return a summary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from control_plane.kernelq.idempotency_keys import dispatch_key
from control_plane.kernelq.idempotency_store import IdempotencyStore, InMemoryIdempotencyStore
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.kafka_producer import DispatchEvent
from control_plane.kernelq.logging_utils import format_log_event

# Default TTL for dispatch dedupe keys (24 hours).
DEFAULT_DISPATCH_DEDUPE_TTL_SECONDS = 86400


@dataclass
class SchedulerTickResult:
    """
    Summary of a single scheduler tick.

    - selected_count: how many jobs were claimed this tick
    - dispatched_count: how many were successfully marked ``dispatched``
    - dispatched_job_ids: ids that moved to ``dispatched`` (in scheduling order)
    - skipped_count: selected jobs that were not dispatched and did not error
      (for example another process already claimed the row)
    - errors: human-readable problem descriptions when the repository call fails
    - published_count: how many dispatch events were published to Kafka
    - publish_errors: per-job Kafka publish failures (claim may still succeed)
    - duplicate_dispatches: dispatch publishes skipped by idempotency dedupe
    """

    selected_count: int
    dispatched_count: int
    dispatched_job_ids: list[str] = field(default_factory=list)
    skipped_count: int = 0
    errors: list[str] = field(default_factory=list)
    published_count: int = 0
    publish_errors: list[str] = field(default_factory=list)
    duplicate_dispatches: int = 0


class SchedulerTickRunner:
    """
    Run one database-backed scheduler tick using ``JobRepository``.

    Typical use (from a loop or cron in the control plane):

        runner = SchedulerTickRunner(repository, max_jobs_per_tick=10, job_producer=producer)
        result = runner.run_once()
        # inspect result.dispatched_count, result.published_count, result.errors, etc.

    Pass ``job_producer=None`` (default) to claim in Postgres only—same as before
    Kafka integration. Pass a ``KafkaJobProducer`` (or test fake) to publish after claim.

    ``max_jobs_per_tick`` caps how many rows we claim per pass so one tick cannot
    overload downstream systems once Kafka and workers are connected.
    """

    def __init__(
        self,
        repository: JobRepository,
        max_jobs_per_tick: int = 10,
        job_producer: Any | None = None,
        idempotency_store: IdempotencyStore | None = None,
        dispatch_dedupe_ttl_seconds: int = DEFAULT_DISPATCH_DEDUPE_TTL_SECONDS,
    ) -> None:
        if max_jobs_per_tick <= 0:
            raise ValueError("max_jobs_per_tick must be a positive integer")

        self._repository = repository
        self._max_jobs_per_tick = max_jobs_per_tick
        self._job_producer = job_producer
        self._idempotency_store = (
            idempotency_store
            if idempotency_store is not None
            else InMemoryIdempotencyStore()
        )
        self._dispatch_dedupe_ttl_seconds = dispatch_dedupe_ttl_seconds

    def run_once(self) -> SchedulerTickResult:
        """
        Execute one scheduler tick.

        1. ``claim_schedulable_jobs`` — one Postgres transaction with
           ``FOR UPDATE SKIP LOCKED`` (no duplicate claim across schedulers).
        2. For each claimed row, optionally ``publish_dispatch_event`` to Kafka.

        **Known reliability gap:** we claim in Postgres *before* publishing.
        If Kafka publish fails, the job stays ``dispatched`` in the DB but may
        never reach workers. We do **not** roll back the DB row yet. A later
        milestone will fix this with an **outbox-style pattern** or a
        **retryable dispatch mechanism** (reconcile / republish / revert state).
        """
        try:
            claimed_jobs = self._repository.claim_schedulable_jobs(
                limit=self._max_jobs_per_tick
            )
        except Exception as exc:
            return SchedulerTickResult(
                selected_count=0,
                dispatched_count=0,
                dispatched_job_ids=[],
                skipped_count=0,
                errors=[f"claim_schedulable_jobs: {type(exc).__name__}: {exc}"],
            )

        dispatched_job_ids = [job.job_id for job in claimed_jobs]
        claimed_count = len(claimed_jobs)
        published_count = 0
        duplicate_dispatches = 0
        publish_errors: list[str] = []

        if self._job_producer is not None:
            for job in claimed_jobs:
                dedupe_key = dispatch_key(job.job_id, job.retry_count)
                if not self._idempotency_store.try_claim(
                    dedupe_key,
                    ttl_seconds=self._dispatch_dedupe_ttl_seconds,
                ):
                    duplicate_dispatches += 1
                    print(
                        format_log_event(
                            "duplicate_dispatch",
                            attempt=job.retry_count,
                            job_id=job.job_id,
                        )
                    )
                    continue

                event = DispatchEvent(
                    event_type="job.dispatch",
                    job_id=job.job_id,
                    tenant_id=job.tenant_id,
                    priority=job.priority,
                    state=job.state,
                    payload=job.payload,
                )
                try:
                    self._job_producer.publish_dispatch_event(event)
                    published_count += 1
                except Exception as exc:
                    # Keep going: other jobs may still publish successfully.
                    publish_errors.append(
                        f"publish {job.job_id}: {type(exc).__name__}: {exc}"
                    )

        return SchedulerTickResult(
            selected_count=claimed_count,
            dispatched_count=claimed_count,
            dispatched_job_ids=dispatched_job_ids,
            skipped_count=0,
            errors=[],
            published_count=published_count,
            publish_errors=publish_errors,
            duplicate_dispatches=duplicate_dispatches,
        )
