"""
Integration tests for SchedulerTickRunner against local Postgres.

Requires:
- ``docker compose up -d postgres`` (container ``kernelq-postgres``)
- Migration ``control_plane/migrations/001_create_jobs.sql`` applied once

Tests use very high priorities so our rows sort ahead of unrelated queued jobs
that may already exist in a shared local database.

``run_once()`` uses ``claim_schedulable_jobs`` (one atomic claim per tick).
Kafka publish tests inject ``FakeJobProducer`` so no real broker is required.
"""

from __future__ import annotations

import time
import uuid

import pytest
from psycopg import OperationalError

from control_plane.kernelq.db import connect
from control_plane.kernelq.idempotency_keys import dispatch_key
from control_plane.kernelq.idempotency_store import InMemoryIdempotencyStore
from control_plane.kernelq.job_repository import JobRecord, JobRepository
from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.kafka_producer import DispatchEvent
from control_plane.kernelq.scheduler_tick import SchedulerTickRunner


TEST_PREFIX = "test-tick-"


class FakeJobProducer:
    """
    Stand-in for ``KafkaJobProducer`` in scheduler tick tests.

    Records every ``DispatchEvent`` passed to ``publish_dispatch_event``.
    Optionally raises for specific ``job_id`` values to simulate broker failures.
    """

    def __init__(self, fail_job_ids: set[str] | None = None) -> None:
        self.published_events: list[DispatchEvent] = []
        self._fail_job_ids = fail_job_ids or set()

    def publish_dispatch_event(self, event: DispatchEvent) -> None:
        if event.job_id in self._fail_job_ids:
            raise RuntimeError(f"simulated publish failure for {event.job_id}")
        self.published_events.append(event)


def _unique_prefix(name: str) -> str:
    # Every job id in this file starts with TEST_PREFIX so cleanup is easy.
    return f"{TEST_PREFIX}{name}_{uuid.uuid4().hex[:16]}"


def _job_id(prefix: str, suffix: str) -> str:
    return f"{prefix}_{suffix}"


def _delete_jobs(repo: JobRepository, *job_ids: str) -> None:
    for job_id in job_ids:
        repo.delete_job(job_id)


def _cleanup_jobs_with_test_prefix(conn) -> None:
    """Delete rows created by this test file only."""
    conn.execute(
        "DELETE FROM jobs WHERE job_id LIKE %(job_id_prefix)s",
        {"job_id_prefix": f"{TEST_PREFIX}%"},
    )
    conn.commit()


def _our_dispatched_ids(dispatched_job_ids: list[str], prefix: str) -> list[str]:
    """Job ids from this test only (shared Postgres may contain other jobs)."""
    return [job_id for job_id in dispatched_job_ids if job_id.startswith(prefix)]


def _our_published_events(events: list[DispatchEvent], prefix: str) -> list[DispatchEvent]:
    """Dispatch events from this test only (ignore unrelated claimed rows)."""
    return [event for event in events if event.job_id.startswith(prefix)]


@pytest.fixture(scope="module", autouse=True)
def _require_postgres() -> None:
    try:
        with connect() as conn:
            conn.execute("SELECT 1")
    except OperationalError as exc:
        pytest.skip(f"Postgres not reachable (start docker compose): {exc}")


@pytest.fixture(autouse=True)
def _cleanup_between_tests() -> None:
    """
    Run cleanup before and after each test.

    This keeps tests isolated even when local Postgres already contains seed
    data or leftovers from previous test runs.
    """
    with connect() as conn:
        _cleanup_jobs_with_test_prefix(conn)
    yield
    with connect() as conn:
        _cleanup_jobs_with_test_prefix(conn)


def test_run_once_dispatches_queued_jobs() -> None:
    prefix = _unique_prefix("test_tick_dispatch_two")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    third_id = _job_id(prefix, "third")
    # High (but valid INT) priority so our rows win ordering in shared DB.
    base = 1_800_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id, third_id)
        try:
            repo.create_job(first_id, "tenant-a", base + 1, JobState.QUEUED.value)
            time.sleep(0.01)
            repo.create_job(second_id, "tenant-a", base + 2, JobState.QUEUED.value)
            time.sleep(0.01)
            repo.create_job(third_id, "tenant-a", base + 3, JobState.QUEUED.value)

            result = SchedulerTickRunner(repo, max_jobs_per_tick=2).run_once()

            ours = _our_dispatched_ids(result.dispatched_job_ids, prefix)
            assert len(ours) == 2
            assert set(ours) == {third_id, second_id}
            assert result.selected_count == result.dispatched_count == 2
            assert result.skipped_count == 0
            assert result.errors == []

            assert repo.get_job(third_id).state == JobState.DISPATCHED.value
            assert repo.get_job(second_id).state == JobState.DISPATCHED.value
            assert repo.get_job(first_id).state == JobState.QUEUED.value
        finally:
            _delete_jobs(repo, first_id, second_id, third_id)


def test_run_once_respects_max_jobs_per_tick() -> None:
    prefix = _unique_prefix("test_tick_limit_one")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    third_id = _job_id(prefix, "third")
    # High (but valid INT) priority so our rows win ordering in shared DB.
    base = 1_810_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id, third_id)
        try:
            repo.create_job(first_id, "tenant-a", base + 1, JobState.QUEUED.value)
            repo.create_job(second_id, "tenant-a", base + 2, JobState.QUEUED.value)
            repo.create_job(third_id, "tenant-a", base + 3, JobState.QUEUED.value)

            result = SchedulerTickRunner(repo, max_jobs_per_tick=1).run_once()

            ours = _our_dispatched_ids(result.dispatched_job_ids, prefix)
            assert ours == [third_id]
            assert result.selected_count == 1
            assert result.dispatched_count == 1
            assert result.skipped_count == 0

            assert repo.get_job(third_id).state == JobState.DISPATCHED.value
            assert repo.get_job(second_id).state == JobState.QUEUED.value
            assert repo.get_job(first_id).state == JobState.QUEUED.value
        finally:
            _delete_jobs(repo, first_id, second_id, third_id)


def test_run_once_ignores_non_queued_jobs() -> None:
    prefix = _unique_prefix("test_tick_skip_failed")
    queued_id = _job_id(prefix, "queued")
    failed_id = _job_id(prefix, "failed")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, queued_id, failed_id)
        try:
            # High priority keeps this queued row in the selected set.
            repo.create_job(queued_id, "tenant-a", 1_820_000_000, JobState.QUEUED.value)
            repo.create_job(failed_id, "tenant-a", 1_820_000_000, JobState.FAILED.value)

            result = SchedulerTickRunner(repo, max_jobs_per_tick=10).run_once()

            ours = _our_dispatched_ids(result.dispatched_job_ids, prefix)
            assert ours == [queued_id]
            assert failed_id not in result.dispatched_job_ids

            assert repo.get_job(queued_id).state == JobState.DISPATCHED.value
            assert repo.get_job(failed_id).state == JobState.FAILED.value
        finally:
            _delete_jobs(repo, queued_id, failed_id)


def test_run_once_twice_does_not_redispatch_same_jobs() -> None:
    """Atomic claim ensures a second tick does not pick the same rows again."""
    prefix = _unique_prefix("test_tick_twice")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    # High (but valid INT) priority so our rows are claimed first.
    base = 1_830_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id)
        try:
            repo.create_job(first_id, "tenant-a", base + 1, JobState.QUEUED.value)
            repo.create_job(second_id, "tenant-a", base + 2, JobState.QUEUED.value)

            runner = SchedulerTickRunner(repo, max_jobs_per_tick=2)

            first = runner.run_once()
            second = runner.run_once()

            first_ours = _our_dispatched_ids(first.dispatched_job_ids, prefix)
            second_ours = _our_dispatched_ids(second.dispatched_job_ids, prefix)

            assert set(first_ours) == {first_id, second_id}
            assert first.selected_count == first.dispatched_count == 2
            assert first.skipped_count == 0

            assert second_ours == []
            assert second.selected_count == second.dispatched_count
            # Second tick may claim unrelated queued rows in a shared DB; our jobs stay dispatched.
            assert repo.get_job(first_id).state == JobState.DISPATCHED.value
            assert repo.get_job(second_id).state == JobState.DISPATCHED.value
        finally:
            _delete_jobs(repo, first_id, second_id)


def test_max_jobs_per_tick_must_be_positive() -> None:
    with connect() as conn:
        repo = JobRepository(conn)
        with pytest.raises(ValueError, match="max_jobs_per_tick"):
            SchedulerTickRunner(repo, max_jobs_per_tick=0)


def test_run_once_does_not_dispatch_non_queued_only_test_rows() -> None:
    """
    In a shared DB we cannot assume zero global queued jobs.

    Instead, verify that if this test creates only non-queued rows, none of
    those rows are dispatched by a tick.
    """
    prefix = _unique_prefix("test_tick_non_queued_only")
    failed_id = _job_id(prefix, "failed")
    succeeded_id = _job_id(prefix, "succeeded")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, failed_id, succeeded_id)
        try:
            repo.create_job(failed_id, "tenant-a", 1_840_000_000, JobState.FAILED.value)
            repo.create_job(
                succeeded_id, "tenant-a", 1_840_000_000, JobState.SUCCEEDED.value
            )

            result = SchedulerTickRunner(repo, max_jobs_per_tick=5).run_once()

            # No non-queued rows from this test should be dispatched.
            ours = _our_dispatched_ids(result.dispatched_job_ids, prefix)
            assert ours == []
            assert repo.get_job(failed_id).state == JobState.FAILED.value
            assert repo.get_job(succeeded_id).state == JobState.SUCCEEDED.value
            assert result.errors == []
        finally:
            _delete_jobs(repo, failed_id, succeeded_id)


def test_run_once_publishes_one_event_per_claimed_job_with_producer() -> None:
    prefix = _unique_prefix("test_tick_publish_count")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    base = 1_850_000_000
    fake = FakeJobProducer()
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id)
        try:
            repo.create_job(first_id, "tenant-a", base + 1, JobState.QUEUED.value)
            time.sleep(0.01)
            repo.create_job(second_id, "tenant-a", base + 2, JobState.QUEUED.value)

            result = SchedulerTickRunner(
                repo, max_jobs_per_tick=2, job_producer=fake
            ).run_once()

            ours = _our_dispatched_ids(result.dispatched_job_ids, prefix)
            published = _our_published_events(fake.published_events, prefix)

            assert len(ours) == 2
            assert len(published) == 2
            assert {event.job_id for event in published} == set(ours)
        finally:
            _delete_jobs(repo, first_id, second_id)


def test_run_once_published_events_include_expected_fields() -> None:
    prefix = _unique_prefix("test_tick_publish_fields")
    job_id = _job_id(prefix, "job")
    payload = {"kind": "billing-export", "amount": 42}
    fake = FakeJobProducer()
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, job_id)
        try:
            repo.create_job(
                job_id,
                "tenant-b",
                1_851_000_000,
                JobState.QUEUED.value,
                payload=payload,
            )

            SchedulerTickRunner(repo, max_jobs_per_tick=1, job_producer=fake).run_once()

            published = _our_published_events(fake.published_events, prefix)
            assert len(published) == 1
            event = published[0]
            assert event.event_type == "job.dispatch"
            assert event.job_id == job_id
            assert event.tenant_id == "tenant-b"
            assert event.priority == 1_851_000_000
            assert event.state == JobState.DISPATCHED.value
            assert event.payload == payload
        finally:
            _delete_jobs(repo, job_id)


def test_run_once_published_count_matches_successful_publishes() -> None:
    prefix = _unique_prefix("test_tick_published_count")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    base = 1_852_000_000
    fake = FakeJobProducer()
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id)
        try:
            repo.create_job(first_id, "tenant-a", base + 1, JobState.QUEUED.value)
            repo.create_job(second_id, "tenant-a", base + 2, JobState.QUEUED.value)

            result = SchedulerTickRunner(
                repo, max_jobs_per_tick=2, job_producer=fake
            ).run_once()

            ours = _our_dispatched_ids(result.dispatched_job_ids, prefix)
            assert len(ours) == 2
            assert result.published_count == 2
            assert len(_our_published_events(fake.published_events, prefix)) == 2
        finally:
            _delete_jobs(repo, first_id, second_id)


def test_run_once_continues_after_publish_failure_for_one_job() -> None:
    prefix = _unique_prefix("test_tick_publish_partial_fail")
    fail_id = _job_id(prefix, "fail")
    ok_id = _job_id(prefix, "ok")
    base = 1_853_000_000
    fake = FakeJobProducer(fail_job_ids={fail_id})
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, fail_id, ok_id)
        try:
            # Higher priority is claimed first; simulate failure on that row.
            repo.create_job(fail_id, "tenant-a", base + 2, JobState.QUEUED.value)
            time.sleep(0.01)
            repo.create_job(ok_id, "tenant-a", base + 1, JobState.QUEUED.value)

            result = SchedulerTickRunner(
                repo, max_jobs_per_tick=2, job_producer=fake
            ).run_once()

            ours = _our_dispatched_ids(result.dispatched_job_ids, prefix)
            published = _our_published_events(fake.published_events, prefix)

            assert set(ours) == {fail_id, ok_id}
            assert result.dispatched_count == 2
            assert result.published_count == 1
            assert len(published) == 1
            assert published[0].job_id == ok_id

            assert len(result.publish_errors) == 1
            assert fail_id in result.publish_errors[0]
            assert "RuntimeError" in result.publish_errors[0]

            assert repo.get_job(fail_id).state == JobState.DISPATCHED.value
            assert repo.get_job(ok_id).state == JobState.DISPATCHED.value
        finally:
            _delete_jobs(repo, fail_id, ok_id)


def test_run_once_without_producer_does_not_publish() -> None:
    prefix = _unique_prefix("test_tick_no_producer")
    job_id = _job_id(prefix, "job")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, job_id)
        try:
            repo.create_job(job_id, "tenant-a", 1_854_000_000, JobState.QUEUED.value)

            result = SchedulerTickRunner(repo, max_jobs_per_tick=1).run_once()

            ours = _our_dispatched_ids(result.dispatched_job_ids, prefix)
            assert ours == [job_id]
            assert result.dispatched_count == 1
            assert result.published_count == 0
            assert result.publish_errors == []
            assert repo.get_job(job_id).state == JobState.DISPATCHED.value
        finally:
            _delete_jobs(repo, job_id)


def _job_record(
    job_id: str,
    *,
    retry_count: int = 0,
    tenant_id: str = "tenant-a",
    priority: int = 1,
) -> JobRecord:
    return JobRecord(
        job_id=job_id,
        tenant_id=tenant_id,
        priority=priority,
        state=JobState.DISPATCHED.value,
        payload={},
        retry_count=retry_count,
        max_retries=3,
        created_at=0,
        updated_at=0,
        dispatched_at=None,
    )


class _FakeClaimRepository:
    """Returns a fixed list from ``claim_schedulable_jobs`` (no Postgres)."""

    def __init__(self, jobs: list[JobRecord]) -> None:
        self._jobs = jobs

    def claim_schedulable_jobs(self, limit: int) -> list[JobRecord]:
        return self._jobs[:limit]


class _TrackingIdempotencyStore(InMemoryIdempotencyStore):
    def __init__(self) -> None:
        super().__init__()
        self.claim_calls: list[tuple[str, int]] = []

    def try_claim(self, key: str, ttl_seconds: int) -> bool:
        self.claim_calls.append((key, ttl_seconds))
        return super().try_claim(key, ttl_seconds)


def test_first_dispatch_publishes() -> None:
    job = _job_record("job-first-dispatch")
    fake = FakeJobProducer()
    store = InMemoryIdempotencyStore()
    result = SchedulerTickRunner(
        _FakeClaimRepository([job]),
        job_producer=fake,
        idempotency_store=store,
    ).run_once()

    assert len(fake.published_events) == 1
    assert result.published_count == 1
    assert result.duplicate_dispatches == 0


def test_duplicate_dispatch_skips_publish() -> None:
    job = _job_record("job-dup-dispatch")
    fake = FakeJobProducer()
    store = InMemoryIdempotencyStore()
    runner = SchedulerTickRunner(
        _FakeClaimRepository([job]),
        job_producer=fake,
        idempotency_store=store,
    )

    first = runner.run_once()
    second = runner.run_once()

    assert len(fake.published_events) == 1
    assert first.published_count == 1
    assert second.published_count == 0
    assert second.duplicate_dispatches == 1


def test_duplicate_dispatches_increments() -> None:
    job = _job_record("job-dup-counter")
    fake = FakeJobProducer()
    store = InMemoryIdempotencyStore()
    runner = SchedulerTickRunner(
        _FakeClaimRepository([job]),
        job_producer=fake,
        idempotency_store=store,
    )

    runner.run_once()
    second = runner.run_once()
    third = runner.run_once()

    assert second.duplicate_dispatches == 1
    assert third.duplicate_dispatches == 1
    assert len(fake.published_events) == 1


def test_published_count_does_not_increment_for_duplicate() -> None:
    job = _job_record("job-no-publish-on-dup")
    fake = FakeJobProducer()
    runner = SchedulerTickRunner(
        _FakeClaimRepository([job]),
        job_producer=fake,
        idempotency_store=InMemoryIdempotencyStore(),
    )

    first = runner.run_once()
    second = runner.run_once()

    assert first.published_count == 1
    assert second.published_count == 0


def test_different_attempts_publish_separately() -> None:
    job_id = "job-attempts"
    attempt0 = _job_record(job_id, retry_count=0)
    attempt1 = _job_record(job_id, retry_count=1)
    fake = FakeJobProducer()
    store = InMemoryIdempotencyStore()
    runner = SchedulerTickRunner(
        _FakeClaimRepository([attempt0, attempt1]),
        max_jobs_per_tick=2,
        job_producer=fake,
        idempotency_store=store,
    )

    result = runner.run_once()

    assert len(fake.published_events) == 2
    assert result.published_count == 2
    assert result.duplicate_dispatches == 0


def test_different_job_ids_publish_separately() -> None:
    first = _job_record("job-a-dispatch")
    second = _job_record("job-b-dispatch")
    fake = FakeJobProducer()
    result = SchedulerTickRunner(
        _FakeClaimRepository([first, second]),
        max_jobs_per_tick=2,
        job_producer=fake,
        idempotency_store=InMemoryIdempotencyStore(),
    ).run_once()

    assert len(fake.published_events) == 2
    assert result.published_count == 2
    assert result.duplicate_dispatches == 0


def test_custom_store_called_with_dispatch_key() -> None:
    job = _job_record("job-dispatch-key", retry_count=2)
    store = _TrackingIdempotencyStore()
    SchedulerTickRunner(
        _FakeClaimRepository([job]),
        job_producer=FakeJobProducer(),
        idempotency_store=store,
    ).run_once()

    assert store.claim_calls == [
        (dispatch_key(job.job_id, job.retry_count), 86400),
    ]
