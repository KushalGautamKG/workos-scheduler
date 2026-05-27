"""
Integration tests for SchedulerTickRunner against local Postgres.

Requires:
- ``docker compose up -d postgres`` (container ``kernelq-postgres``)
- Migration ``control_plane/migrations/001_create_jobs.sql`` applied once

Tests use very high priorities so our rows sort ahead of unrelated queued jobs
that may already exist in a shared local database.

``run_once()`` uses ``claim_schedulable_jobs`` (one atomic claim per tick).
"""

from __future__ import annotations

import time
import uuid

import pytest
from psycopg import OperationalError

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState
from control_plane.kernelq.scheduler_tick import SchedulerTickRunner


TEST_PREFIX = "test-tick-"


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
