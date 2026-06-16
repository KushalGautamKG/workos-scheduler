"""
Integration tests for JobRepository against local Postgres.

Requires:
- ``docker compose up -d postgres`` (container ``kernelq-postgres``)
- Migration ``control_plane/migrations/001_create_jobs.sql`` applied once

Each test uses a unique ``job_id`` and deletes it in ``finally`` so runs stay isolated.
"""

from __future__ import annotations

import time
import uuid

import pytest
from psycopg import OperationalError

from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState


TEST_PREFIX = "test-repo-"


def _unique_job_id(prefix: str) -> str:
    # All job ids created in this file start with TEST_PREFIX so we can clean
    # up safely in a shared Postgres environment.
    return f"{TEST_PREFIX}{prefix}_{uuid.uuid4().hex[:16]}"


def _job_id(prefix: str, suffix: str) -> str:
    """Build a unique job id under one test prefix (easy cleanup and filtering)."""
    return f"{prefix}_{suffix}"


def _our_jobs(results: list, prefix: str) -> list:
    """Rows from this test only (shared Postgres may contain other jobs)."""
    return [job for job in results if job.job_id.startswith(prefix)]


def _our_dead_lettered_jobs(results: list[dict], prefix: str) -> list[dict]:
    """Dead-letter dict rows from this test only (shared Postgres may contain other jobs)."""
    return [job for job in results if job["job_id"].startswith(prefix)]


def _delete_jobs(repo: JobRepository, *job_ids: str) -> None:
    for job_id in job_ids:
        repo.delete_job(job_id)


def _cleanup_jobs_with_test_prefix(conn) -> None:
    """Delete any rows created by this test file (based on TEST_PREFIX)."""
    conn.execute(
        "DELETE FROM jobs WHERE job_id LIKE %(job_id_prefix)s",
        {"job_id_prefix": f"{TEST_PREFIX}%"},
    )
    conn.commit()


@pytest.fixture(scope="module", autouse=True)
def _require_postgres() -> None:
    try:
        with connect() as conn:
            conn.execute("SELECT 1")
    except OperationalError as exc:
        pytest.skip(f"Postgres not reachable (start docker compose): {exc}")


@pytest.fixture(scope="module", autouse=True)
def _ensure_retry_after_column(_require_postgres) -> None:
    """
    ``requeue_due_retries`` needs ``retry_after`` on ``jobs``.

    Apply the column locally if missing (safe ``IF NOT EXISTS`` for shared dev DBs).
    """
    with connect() as conn:
        conn.execute(
            """
            ALTER TABLE jobs
            ADD COLUMN IF NOT EXISTS retry_after BIGINT
            """
        )
        conn.commit()


def _set_retry_scheduled(
    conn,
    job_id: str,
    retry_after: int,
    *,
    state: str = JobState.RETRY_SCHEDULED.value,
) -> None:
    """Put a job in retry_scheduled with a concrete ``retry_after`` timestamp."""
    conn.execute(
        """
        UPDATE jobs
        SET state = %(state)s, retry_after = %(retry_after)s, updated_at = NOW()
        WHERE job_id = %(job_id)s
        """,
        {"job_id": job_id, "state": state, "retry_after": retry_after},
    )
    conn.commit()


def _set_retry_count(conn, job_id: str, retry_count: int) -> None:
    """Set ``retry_count`` on an existing job row (test setup helper)."""
    conn.execute(
        """
        UPDATE jobs
        SET retry_count = %(retry_count)s, updated_at = NOW()
        WHERE job_id = %(job_id)s
        """,
        {"job_id": job_id, "retry_count": retry_count},
    )
    conn.commit()


def _set_job_state_and_updated_at(
    conn,
    job_id: str,
    state: str,
    updated_at: str,
) -> None:
    """Set ``state`` and a fixed ``updated_at`` (ordering tests in shared Postgres)."""
    conn.execute(
        """
        UPDATE jobs
        SET state = %(state)s, updated_at = %(updated_at)s::timestamptz
        WHERE job_id = %(job_id)s
        """,
        {"job_id": job_id, "state": state, "updated_at": updated_at},
    )
    conn.commit()


def _fetch_retry_after(conn, job_id: str) -> int | None:
    """Read ``retry_after`` for one job (None if column is SQL NULL)."""
    row = conn.execute(
        "SELECT retry_after FROM jobs WHERE job_id = %(job_id)s",
        {"job_id": job_id},
    ).fetchone()
    if row is None:
        return None
    return row[0]


@pytest.fixture(autouse=True)
def _cleanup_between_tests() -> None:
    """
    Keep tests isolated even when the local Postgres container already has
    other data (seed data, earlier runs, etc.).
    """
    with connect() as conn:
        _cleanup_jobs_with_test_prefix(conn)
    yield
    with connect() as conn:
        _cleanup_jobs_with_test_prefix(conn)


def test_create_job_inserts_and_returns_record() -> None:
    job_id = _unique_job_id("test_jr_create")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            rec = repo.create_job(
                job_id,
                tenant_id="tenant-a",
                priority=7,
                state="queued",
                payload={"kind": "demo"},
                max_retries=5,
            )

            assert rec.job_id == job_id
            assert rec.tenant_id == "tenant-a"
            assert rec.priority == 7
            assert rec.state == "queued"
            assert rec.payload == {"kind": "demo"}
            assert rec.retry_count == 0
            assert rec.max_retries == 5
            assert rec.created_at is not None
            assert rec.updated_at is not None
        finally:
            repo.delete_job(job_id)


def test_get_job_returns_existing_job() -> None:
    job_id = _unique_job_id("test_jr_get")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(job_id, "tenant-b", 3, "queued", payload={})

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.job_id == job_id
            assert loaded.tenant_id == "tenant-b"
            assert loaded.priority == 3
            assert loaded.state == "queued"
            assert loaded.payload == {}
        finally:
            repo.delete_job(job_id)


def test_get_job_returns_none_for_missing_job() -> None:
    missing_id = _unique_job_id("test_jr_missing")
    with connect() as conn:
        repo = JobRepository(conn)
        assert repo.get_job(missing_id) is None


def test_update_job_state_changes_state() -> None:
    job_id = _unique_job_id("test_jr_update")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(job_id, "tenant-a", 1, "queued")

            updated = repo.update_job_state(job_id, "canceled")
            assert updated is not None
            assert updated.state == "canceled"
            assert updated.job_id == job_id

            again = repo.get_job(job_id)
            assert again is not None
            assert again.state == "canceled"
        finally:
            repo.delete_job(job_id)


def test_update_job_state_from_worker_result_succeeds_and_persists() -> None:
    job_id = _unique_job_id("test_jr_worker_result_ok")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(job_id, "tenant-a", 1, JobState.RUNNING.value)

            updated = repo.update_job_state_from_worker_result(
                job_id,
                JobState.SUCCEEDED.value,
            )

            assert updated is True

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.SUCCEEDED.value
        finally:
            repo.delete_job(job_id)


def test_update_job_state_from_worker_result_missing_returns_false() -> None:
    missing_id = _unique_job_id("test_jr_worker_result_missing")
    with connect() as conn:
        repo = JobRepository(conn)
        assert (
            repo.update_job_state_from_worker_result(
                missing_id,
                JobState.SUCCEEDED.value,
            )
            is False
        )
        assert repo.get_job(missing_id) is None


def test_schedule_retry_below_max_increments_and_sets_retry_scheduled() -> None:
    """retry_count < max_retries → increment, RETRY_SCHEDULED."""
    job_id = _unique_job_id("test_jr_sched_below_max")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(
                job_id,
                "tenant-a",
                1,
                JobState.RUNNING.value,
                max_retries=3,
            )

            result = repo.schedule_retry_from_worker_result(job_id)

            assert result is not None
            assert result.outcome == "scheduled"
            assert result.state == JobState.RETRY_SCHEDULED.value
            assert result.retry_count == 1
            assert result.max_retries == 3

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.RETRY_SCHEDULED.value
            assert loaded.retry_count == 1
        finally:
            repo.delete_job(job_id)


def test_schedule_retry_at_max_sets_dead_lettered() -> None:
    """retry_count == max_retries → DEAD_LETTERED (no increment)."""
    job_id = _unique_job_id("test_jr_sched_at_max")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(
                job_id,
                "tenant-a",
                1,
                JobState.RUNNING.value,
                max_retries=3,
            )
            _set_retry_count(conn, job_id, retry_count=3)

            result = repo.schedule_retry_from_worker_result(job_id)

            assert result is not None
            assert result.outcome == "exhausted"
            assert result.state == JobState.DEAD_LETTERED.value
            assert result.retry_count == 3

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.DEAD_LETTERED.value
            assert loaded.retry_count == 3
        finally:
            repo.delete_job(job_id)


def test_schedule_retry_above_max_sets_dead_lettered() -> None:
    """retry_count > max_retries → DEAD_LETTERED (count unchanged)."""
    job_id = _unique_job_id("test_jr_sched_above_max")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(
                job_id,
                "tenant-a",
                1,
                JobState.RUNNING.value,
                max_retries=2,
            )
            _set_retry_count(conn, job_id, retry_count=5)

            result = repo.schedule_retry_from_worker_result(job_id)

            assert result is not None
            assert result.outcome == "exhausted"
            assert result.state == JobState.DEAD_LETTERED.value
            assert result.retry_count == 5

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.DEAD_LETTERED.value
            assert loaded.retry_count == 5
        finally:
            repo.delete_job(job_id)


def test_schedule_retry_missing_returns_none() -> None:
    missing_id = _unique_job_id("test_jr_sched_missing")
    with connect() as conn:
        repo = JobRepository(conn)
        assert repo.schedule_retry_from_worker_result(missing_id) is None
        assert repo.get_job(missing_id) is None


def test_schedule_retry_sets_retry_after_when_scheduled() -> None:
    """Scheduled retries get retry_after = now + delay."""
    job_id = _unique_job_id("test_jr_sched_retry_after")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(
                job_id,
                "tenant-a",
                1,
                JobState.RUNNING.value,
                max_retries=3,
            )

            before = int(time.time())
            result = repo.schedule_retry_from_worker_result(
                job_id,
                retry_delay_seconds=45,
            )
            after = int(time.time())

            assert result is not None
            assert result.outcome == "scheduled"
            assert result.retry_after is not None
            assert before + 45 <= result.retry_after <= after + 45
            assert _fetch_retry_after(conn, job_id) == result.retry_after
        finally:
            repo.delete_job(job_id)


def test_schedule_retry_exhausted_preserves_retry_after() -> None:
    """Exhausted retries do not require a new retry_after (leave existing value)."""
    job_id = _unique_job_id("test_jr_sched_exhaust_after")
    stale_retry_after = 1_600_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(
                job_id,
                "tenant-a",
                1,
                JobState.RUNNING.value,
                max_retries=1,
            )
            _set_retry_scheduled(conn, job_id, retry_after=stale_retry_after)
            _set_retry_count(conn, job_id, retry_count=1)

            result = repo.schedule_retry_from_worker_result(
                job_id,
                retry_delay_seconds=999,
            )

            assert result is not None
            assert result.outcome == "exhausted"
            assert result.state == JobState.DEAD_LETTERED.value
            # retry_after stays the old timestamp — not bumped to now + delay.
            assert _fetch_retry_after(conn, job_id) == stale_retry_after
            assert result.retry_after == stale_retry_after
        finally:
            repo.delete_job(job_id)


def test_requeue_due_retries_moves_due_job_to_queued() -> None:
    """A retry_scheduled job with retry_after <= now becomes queued."""
    job_id = _unique_job_id("test_jr_requeue_due")
    now = 1_700_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(job_id, "tenant-a", 1, JobState.QUEUED.value)
            _set_retry_scheduled(conn, job_id, retry_after=now - 60)

            requeued = repo.requeue_due_retries(now=now, limit=10)

            assert job_id in requeued
            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.QUEUED.value
        finally:
            repo.delete_job(job_id)


def test_requeue_due_retries_skips_future_retry_after() -> None:
    """Jobs with retry_after in the future stay retry_scheduled."""
    job_id = _unique_job_id("test_jr_requeue_future")
    now = 1_700_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(job_id, "tenant-a", 1, JobState.QUEUED.value)
            _set_retry_scheduled(conn, job_id, retry_after=now + 3600)

            requeued = repo.requeue_due_retries(now=now, limit=10)

            assert requeued == []
            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.RETRY_SCHEDULED.value
        finally:
            repo.delete_job(job_id)


def test_requeue_due_retries_respects_limit() -> None:
    prefix = _unique_job_id("test_jr_requeue_limit")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    third_id = _job_id(prefix, "third")
    now = 1_700_100_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id, third_id)
        try:
            repo.create_job(first_id, "tenant-a", 1, JobState.QUEUED.value)
            repo.create_job(second_id, "tenant-a", 1, JobState.QUEUED.value)
            repo.create_job(third_id, "tenant-a", 1, JobState.QUEUED.value)
            # Same due time; created_at order breaks ties (first inserted first).
            _set_retry_scheduled(conn, first_id, retry_after=now - 10)
            _set_retry_scheduled(conn, second_id, retry_after=now - 10)
            _set_retry_scheduled(conn, third_id, retry_after=now - 10)

            requeued = repo.requeue_due_retries(now=now, limit=2)

            assert len(requeued) == 2
            assert set(requeued) <= {first_id, second_id, third_id}
            queued_count = sum(
                1
                for jid in (first_id, second_id, third_id)
                if repo.get_job(jid).state == JobState.QUEUED.value
            )
            assert queued_count == 2
        finally:
            _delete_jobs(repo, first_id, second_id, third_id)


def test_requeue_due_retries_returns_only_requeued_job_ids() -> None:
    """Returned ids are due jobs only — not future retries or other states."""
    prefix = _unique_job_id("test_jr_requeue_ids")
    due_id = _job_id(prefix, "due")
    future_id = _job_id(prefix, "future")
    queued_id = _job_id(prefix, "queued")
    now = 1_700_200_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, due_id, future_id, queued_id)
        try:
            repo.create_job(due_id, "tenant-a", 1, JobState.QUEUED.value)
            repo.create_job(future_id, "tenant-a", 1, JobState.QUEUED.value)
            repo.create_job(queued_id, "tenant-a", 1, JobState.QUEUED.value)
            _set_retry_scheduled(conn, due_id, retry_after=now)
            _set_retry_scheduled(conn, future_id, retry_after=now + 9999)
            # queued_id stays queued (not retry_scheduled)

            requeued = repo.requeue_due_retries(now=now, limit=10)

            assert requeued == [due_id]
            assert repo.get_job(due_id).state == JobState.QUEUED.value
            assert repo.get_job(future_id).state == JobState.RETRY_SCHEDULED.value
            assert repo.get_job(queued_id).state == JobState.QUEUED.value
        finally:
            _delete_jobs(repo, due_id, future_id, queued_id)


def test_delete_job_removes_job() -> None:
    job_id = _unique_job_id("test_jr_delete")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(job_id, "tenant-a", 2, "queued")

            assert repo.delete_job(job_id) is True
            assert repo.get_job(job_id) is None
            assert repo.delete_job(job_id) is False
        finally:
            repo.delete_job(job_id)


def test_list_schedulable_jobs_returns_only_queued() -> None:
    prefix = _unique_job_id("test_jr_sched_queued_only")
    queued_id = _job_id(prefix, "queued")
    created_id = _job_id(prefix, "created")
    dispatched_id = _job_id(prefix, "dispatched")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, queued_id, created_id, dispatched_id)
        try:
            # Use extremely high priority so these jobs win ordering even if
            # other queued rows exist in a shared local DB.
            # Use a very large priority (still within INT range) so these jobs
            # win ordering even if other queued rows exist in a shared local DB.
            repo.create_job(queued_id, "tenant-a", 1_900_000_000, JobState.QUEUED.value)
            repo.create_job(
                created_id, "tenant-a", 1_900_000_000, JobState.CREATED.value
            )
            repo.create_job(
                dispatched_id, "tenant-a", 1_900_000_000, JobState.DISPATCHED.value
            )

            ours = _our_jobs(repo.list_schedulable_jobs(limit=500), prefix)

            assert len(ours) == 1
            assert ours[0].job_id == queued_id
            assert ours[0].state == JobState.QUEUED.value
        finally:
            _delete_jobs(repo, queued_id, created_id, dispatched_id)


def test_list_schedulable_jobs_orders_by_priority_desc() -> None:
    prefix = _unique_job_id("test_jr_sched_priority")
    low_id = _job_id(prefix, "low")
    mid_id = _job_id(prefix, "mid")
    high_id = _job_id(prefix, "high")
    # High values so these rows sort ahead of unrelated queued jobs in shared Postgres.
    base_priority = 1_900_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, low_id, mid_id, high_id)
        try:
            repo.create_job(low_id, "tenant-a", base_priority + 1, JobState.QUEUED.value)
            repo.create_job(mid_id, "tenant-a", base_priority + 5, JobState.QUEUED.value)
            repo.create_job(high_id, "tenant-a", base_priority + 10, JobState.QUEUED.value)

            ours = _our_jobs(repo.list_schedulable_jobs(limit=10), prefix)

            assert [job.job_id for job in ours] == [high_id, mid_id, low_id]
            assert [job.priority for job in ours] == [
                base_priority + 10,
                base_priority + 5,
                base_priority + 1,
            ]
        finally:
            _delete_jobs(repo, low_id, mid_id, high_id)


def test_list_schedulable_jobs_breaks_priority_ties_by_created_at_asc() -> None:
    prefix = _unique_job_id("test_jr_sched_tie")
    older_id = _job_id(prefix, "older")
    newer_id = _job_id(prefix, "newer")
    # Use a very large priority so we win the top ordering even with other queued rows.
    priority = 1_900_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, older_id, newer_id)
        try:
            repo.create_job(older_id, "tenant-a", priority, JobState.QUEUED.value)
            time.sleep(0.02)
            repo.create_job(newer_id, "tenant-a", priority, JobState.QUEUED.value)

            ours = _our_jobs(repo.list_schedulable_jobs(limit=10), prefix)

            assert len(ours) == 2
            assert ours[0].job_id == older_id
            assert ours[1].job_id == newer_id
            assert ours[0].created_at <= ours[1].created_at
        finally:
            _delete_jobs(repo, older_id, newer_id)


def test_list_schedulable_jobs_respects_limit() -> None:
    prefix = _unique_job_id("test_jr_sched_limit")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    third_id = _job_id(prefix, "third")
    # Ensure our rows are always in the top LIMIT results.
    base_priority = 1_901_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id, third_id)
        try:
            repo.create_job(first_id, "tenant-a", base_priority + 1, JobState.QUEUED.value)
            repo.create_job(second_id, "tenant-a", base_priority + 2, JobState.QUEUED.value)
            repo.create_job(third_id, "tenant-a", base_priority + 3, JobState.QUEUED.value)

            results = repo.list_schedulable_jobs(limit=2)

            assert len(results) == 2
            ours = _our_jobs(results, prefix)
            assert [job.job_id for job in ours] == [third_id, second_id]
        finally:
            _delete_jobs(repo, first_id, second_id, third_id)


def test_list_dead_lettered_jobs_returns_only_dead_lettered() -> None:
    prefix = _unique_job_id("test_jr_dl_only")
    dead_id = _job_id(prefix, "dead")
    queued_id = _job_id(prefix, "queued")
    succeeded_id = _job_id(prefix, "succeeded")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, dead_id, queued_id, succeeded_id)
        try:
            repo.create_job(
                dead_id,
                "tenant-a",
                1,
                JobState.DEAD_LETTERED.value,
                payload={"kind": "dead"},
                max_retries=3,
            )
            _set_retry_count(conn, dead_id, retry_count=3)
            repo.create_job(queued_id, "tenant-a", 1, JobState.QUEUED.value)
            repo.create_job(succeeded_id, "tenant-a", 1, JobState.SUCCEEDED.value)

            ours = _our_dead_lettered_jobs(repo.list_dead_lettered_jobs(limit=500), prefix)

            assert len(ours) == 1
            assert ours[0]["job_id"] == dead_id
            assert ours[0]["state"] == JobState.DEAD_LETTERED.value
        finally:
            _delete_jobs(repo, dead_id, queued_id, succeeded_id)


def test_list_dead_lettered_jobs_respects_limit() -> None:
    prefix = _unique_job_id("test_jr_dl_limit")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    third_id = _job_id(prefix, "third")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id, third_id)
        try:
            repo.create_job(first_id, "tenant-a", 1, JobState.DISPATCHED.value)
            repo.create_job(second_id, "tenant-a", 1, JobState.DISPATCHED.value)
            repo.create_job(third_id, "tenant-a", 1, JobState.DISPATCHED.value)
            # Far-future timestamps so our rows sort ahead of unrelated dead_lettered seed data.
            _set_job_state_and_updated_at(
                conn, first_id, JobState.DEAD_LETTERED.value, "2099-01-01T00:00:00Z"
            )
            _set_job_state_and_updated_at(
                conn, second_id, JobState.DEAD_LETTERED.value, "2099-01-02T00:00:00Z"
            )
            _set_job_state_and_updated_at(
                conn, third_id, JobState.DEAD_LETTERED.value, "2099-01-03T00:00:00Z"
            )

            results = repo.list_dead_lettered_jobs(limit=2)
            ours = _our_dead_lettered_jobs(results, prefix)

            assert len(results) == 2
            assert len(ours) == 2
            assert [job["job_id"] for job in ours] == [third_id, second_id]
        finally:
            _delete_jobs(repo, first_id, second_id, third_id)


def test_list_dead_lettered_jobs_orders_by_updated_at_desc() -> None:
    prefix = _unique_job_id("test_jr_dl_order")
    older_id = _job_id(prefix, "older")
    newer_id = _job_id(prefix, "newer")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, older_id, newer_id)
        try:
            repo.create_job(older_id, "tenant-a", 1, JobState.DISPATCHED.value)
            repo.create_job(newer_id, "tenant-a", 1, JobState.DISPATCHED.value)
            _set_job_state_and_updated_at(
                conn, older_id, JobState.DEAD_LETTERED.value, "2099-06-01T00:00:00Z"
            )
            _set_job_state_and_updated_at(
                conn, newer_id, JobState.DEAD_LETTERED.value, "2099-06-02T00:00:00Z"
            )

            ours = _our_dead_lettered_jobs(repo.list_dead_lettered_jobs(limit=10), prefix)

            assert len(ours) == 2
            assert [job["job_id"] for job in ours] == [newer_id, older_id]
            assert ours[0]["updated_at"] >= ours[1]["updated_at"]
        finally:
            _delete_jobs(repo, older_id, newer_id)


def test_list_dead_lettered_jobs_dict_includes_required_fields() -> None:
    job_id = _unique_job_id("test_jr_dl_fields")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(
                job_id,
                "tenant-a",
                7,
                JobState.DEAD_LETTERED.value,
                payload={"kind": "inspect-me"},
                max_retries=5,
            )
            _set_retry_count(conn, job_id, retry_count=5)

            ours = _our_dead_lettered_jobs(repo.list_dead_lettered_jobs(limit=10), job_id)

            assert len(ours) == 1
            row = ours[0]
            assert row["job_id"] == job_id
            assert row["retry_count"] == 5
            assert row["max_retries"] == 5
            assert row["payload"] == {"kind": "inspect-me"}
            assert row["state"] == JobState.DEAD_LETTERED.value
            assert row["tenant_id"] == "tenant-a"
            assert row["priority"] == 7
            assert row["created_at"] is not None
            assert row["updated_at"] is not None
        finally:
            repo.delete_job(job_id)


def test_requeue_dead_lettered_job_becomes_queued() -> None:
    job_id = _unique_job_id("test_jr_requeue_dl_ok")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(
                job_id,
                "tenant-a",
                1,
                JobState.DEAD_LETTERED.value,
                max_retries=3,
            )
            _set_retry_count(conn, job_id, retry_count=3)

            assert repo.requeue_dead_lettered_job(job_id) is True

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.QUEUED.value
        finally:
            repo.delete_job(job_id)


def test_requeue_dead_lettered_job_preserves_retry_count() -> None:
    job_id = _unique_job_id("test_jr_requeue_dl_count")
    with connect() as conn:
        repo = JobRepository(conn)
        try:
            repo.create_job(
                job_id,
                "tenant-a",
                1,
                JobState.DEAD_LETTERED.value,
                max_retries=5,
            )
            _set_retry_count(conn, job_id, retry_count=5)
            _set_retry_scheduled(
                conn,
                job_id,
                retry_after=1_700_000_000,
                state=JobState.DEAD_LETTERED.value,
            )

            assert repo.requeue_dead_lettered_job(job_id) is True

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.retry_count == 5
            assert loaded.max_retries == 5
            assert _fetch_retry_after(conn, job_id) is None
        finally:
            repo.delete_job(job_id)


def test_requeue_dead_lettered_job_skips_non_dead_lettered() -> None:
    prefix = _unique_job_id("test_jr_requeue_dl_skip")
    queued_id = _job_id(prefix, "queued")
    succeeded_id = _job_id(prefix, "succeeded")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, queued_id, succeeded_id)
        try:
            repo.create_job(queued_id, "tenant-a", 1, JobState.QUEUED.value)
            repo.create_job(succeeded_id, "tenant-a", 1, JobState.SUCCEEDED.value)

            assert repo.requeue_dead_lettered_job(queued_id) is False
            assert repo.requeue_dead_lettered_job(succeeded_id) is False

            assert repo.get_job(queued_id).state == JobState.QUEUED.value
            assert repo.get_job(succeeded_id).state == JobState.SUCCEEDED.value
        finally:
            _delete_jobs(repo, queued_id, succeeded_id)


def test_requeue_dead_lettered_job_missing_returns_false() -> None:
    missing_id = _unique_job_id("test_jr_requeue_dl_missing")
    with connect() as conn:
        repo = JobRepository(conn)
        assert repo.requeue_dead_lettered_job(missing_id) is False
        assert repo.get_job(missing_id) is None


def test_mark_job_dispatched_queued_becomes_dispatched() -> None:
    job_id = _unique_job_id("test_jr_dispatch_ok")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, job_id)
        try:
            repo.create_job(job_id, "tenant-a", 3, JobState.QUEUED.value)

            updated = repo.mark_job_dispatched(job_id)

            assert updated is not None
            assert updated.state == JobState.DISPATCHED.value

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.DISPATCHED.value
        finally:
            _delete_jobs(repo, job_id)


def test_mark_job_dispatched_missing_returns_none() -> None:
    missing_id = _unique_job_id("test_jr_dispatch_missing")
    with connect() as conn:
        repo = JobRepository(conn)
        assert repo.mark_job_dispatched(missing_id) is None


def test_mark_job_dispatched_non_queued_returns_none() -> None:
    job_id = _unique_job_id("test_jr_dispatch_not_queued")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, job_id)
        try:
            repo.create_job(job_id, "tenant-a", 1, JobState.RUNNING.value)

            assert repo.mark_job_dispatched(job_id) is None

            loaded = repo.get_job(job_id)
            assert loaded is not None
            assert loaded.state == JobState.RUNNING.value
        finally:
            _delete_jobs(repo, job_id)


def test_claim_schedulable_jobs_marks_queued_as_dispatched() -> None:
    prefix = _unique_job_id("test_jr_claim_dispatch")
    first_id = _job_id(prefix, "one")
    second_id = _job_id(prefix, "two")
    base = 4_000_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id)
        try:
            repo.create_job(first_id, "tenant-a", base + 1, JobState.QUEUED.value)
            repo.create_job(second_id, "tenant-a", base + 2, JobState.QUEUED.value)

            claimed = repo.claim_schedulable_jobs(limit=2)
            ours = _our_jobs(claimed, prefix)

            assert len(ours) == 2
            assert all(job.state == JobState.DISPATCHED.value for job in ours)
            assert repo.get_job(first_id).state == JobState.DISPATCHED.value
            assert repo.get_job(second_id).state == JobState.DISPATCHED.value
        finally:
            _delete_jobs(repo, first_id, second_id)


def test_claim_schedulable_jobs_respects_limit() -> None:
    prefix = _unique_job_id("test_jr_claim_limit")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    third_id = _job_id(prefix, "third")
    base = 4_100_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id, third_id)
        try:
            repo.create_job(first_id, "tenant-a", base + 1, JobState.QUEUED.value)
            repo.create_job(second_id, "tenant-a", base + 2, JobState.QUEUED.value)
            repo.create_job(third_id, "tenant-a", base + 3, JobState.QUEUED.value)

            claimed = repo.claim_schedulable_jobs(limit=2)
            ours = _our_jobs(claimed, prefix)

            assert len(ours) == 2
            assert [job.job_id for job in ours] == [third_id, second_id]
            assert repo.get_job(third_id).state == JobState.DISPATCHED.value
            assert repo.get_job(second_id).state == JobState.DISPATCHED.value
            assert repo.get_job(first_id).state == JobState.QUEUED.value
        finally:
            _delete_jobs(repo, first_id, second_id, third_id)


def test_claim_schedulable_jobs_returns_only_queued() -> None:
    prefix = _unique_job_id("test_jr_claim_queued_only")
    queued_id = _job_id(prefix, "queued")
    created_id = _job_id(prefix, "created")
    failed_id = _job_id(prefix, "failed")
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, queued_id, created_id, failed_id)
        try:
            repo.create_job(queued_id, "tenant-a", 4_200_000, JobState.QUEUED.value)
            repo.create_job(created_id, "tenant-a", 4_200_000, JobState.CREATED.value)
            repo.create_job(failed_id, "tenant-a", 4_200_000, JobState.FAILED.value)

            claimed = repo.claim_schedulable_jobs(limit=10)
            ours = _our_jobs(claimed, prefix)

            assert len(ours) == 1
            assert ours[0].job_id == queued_id
            assert ours[0].state == JobState.DISPATCHED.value
            assert repo.get_job(failed_id).state == JobState.FAILED.value
            assert repo.get_job(created_id).state == JobState.CREATED.value
        finally:
            _delete_jobs(repo, queued_id, created_id, failed_id)


def test_claim_schedulable_jobs_orders_by_priority_then_created_at() -> None:
    prefix = _unique_job_id("test_jr_claim_order")
    low_id = _job_id(prefix, "low")
    high_new_id = _job_id(prefix, "high_new")
    high_old_id = _job_id(prefix, "high_old")
    base = 4_300_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, low_id, high_new_id, high_old_id)
        try:
            repo.create_job(low_id, "tenant-a", base + 1, JobState.QUEUED.value)
            time.sleep(0.02)
            repo.create_job(high_new_id, "tenant-a", base + 10, JobState.QUEUED.value)
            time.sleep(0.02)
            repo.create_job(high_old_id, "tenant-a", base + 10, JobState.QUEUED.value)

            claimed = repo.claim_schedulable_jobs(limit=10)
            ours = _our_jobs(claimed, prefix)

            assert [job.job_id for job in ours] == [high_new_id, high_old_id, low_id]
            assert [job.priority for job in ours] == [base + 10, base + 10, base + 1]
        finally:
            _delete_jobs(repo, low_id, high_new_id, high_old_id)


def test_claim_schedulable_jobs_limit_must_be_positive() -> None:
    with connect() as conn:
        repo = JobRepository(conn)
        with pytest.raises(ValueError, match="limit"):
            repo.claim_schedulable_jobs(limit=0)


def test_claim_schedulable_jobs_does_not_reclaim_dispatched() -> None:
    prefix = _unique_job_id("test_jr_claim_twice")
    first_id = _job_id(prefix, "first")
    second_id = _job_id(prefix, "second")
    base = 4_400_000
    with connect() as conn:
        repo = JobRepository(conn)
        _delete_jobs(repo, first_id, second_id)
        try:
            repo.create_job(first_id, "tenant-a", base + 1, JobState.QUEUED.value)
            repo.create_job(second_id, "tenant-a", base + 2, JobState.QUEUED.value)

            first_claim = repo.claim_schedulable_jobs(limit=2)
            first_ours = _our_jobs(first_claim, prefix)
            assert len(first_ours) == 2

            second_claim = repo.claim_schedulable_jobs(limit=10)
            second_ours = _our_jobs(second_claim, prefix)
            assert second_ours == []
        finally:
            _delete_jobs(repo, first_id, second_id)
