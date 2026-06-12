"""
Persist and load KernelQ jobs in PostgreSQL.

The API layer should not embed SQL strings everywhere. This small repository
keeps INSERT/SELECT/UPDATE/DELETE in one place and uses parameterized queries
so values are never pasted into SQL as raw strings (safer and clearer).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from control_plane.kernelq.job_state import JobState


@dataclass
class JobRecord:
    """One row from the ``jobs`` table, mapped to Python types."""

    job_id: str
    tenant_id: str
    priority: int
    state: str
    payload: dict[str, Any]
    retry_count: int
    max_retries: int
    created_at: object
    updated_at: object


def _row_to_record(row: dict[str, Any]) -> JobRecord:
    """Build a JobRecord from a dict-shaped query row."""
    raw_payload = row.get("payload")
    if isinstance(raw_payload, dict):
        payload = dict(raw_payload)
    else:
        # JSON objects map to dict; if we ever see something else, keep a safe empty dict.
        payload = {}

    return JobRecord(
        job_id=row["job_id"],
        tenant_id=row["tenant_id"],
        priority=row["priority"],
        state=row["state"],
        payload=payload,
        retry_count=row["retry_count"],
        max_retries=row["max_retries"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class JobRepository:
    """CRUD-style access to the ``jobs`` table using an existing psycopg connection."""

    def __init__(self, conn: Connection) -> None:
        self._conn = conn

    def create_job(
        self,
        job_id: str,
        tenant_id: str,
        priority: int,
        state: str,
        payload: dict[str, Any] | None = None,
        max_retries: int = 3,
    ) -> JobRecord:
        """Insert a new job row and return the stored record (including timestamps)."""
        data = payload if payload is not None else {}

        sql = """
            INSERT INTO jobs (job_id, tenant_id, priority, state, payload, max_retries)
            VALUES (%(job_id)s, %(tenant_id)s, %(priority)s, %(state)s, %(payload)s, %(max_retries)s)
            RETURNING
                job_id, tenant_id, priority, state, payload,
                retry_count, max_retries, created_at, updated_at
        """
        params = {
            "job_id": job_id,
            "tenant_id": tenant_id,
            "priority": priority,
            "state": state,
            "payload": Json(data),
            "max_retries": max_retries,
        }

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            assert row is not None

        self._conn.commit()
        return _row_to_record(row)

    def get_job(self, job_id: str) -> JobRecord | None:
        """Load one job by primary key, or None if it does not exist."""
        sql = """
            SELECT
                job_id, tenant_id, priority, state, payload,
                retry_count, max_retries, created_at, updated_at
            FROM jobs
            WHERE job_id = %(job_id)s
        """

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, {"job_id": job_id})
            row = cur.fetchone()

        self._conn.commit()
        if row is None:
            return None
        return _row_to_record(row)

    def update_job_state(self, job_id: str, new_state: str) -> JobRecord | None:
        """Set ``state`` and bump ``updated_at``; return the row or None if missing."""
        sql = """
            UPDATE jobs
            SET state = %(new_state)s, updated_at = NOW()
            WHERE job_id = %(job_id)s
            RETURNING
                job_id, tenant_id, priority, state, payload,
                retry_count, max_retries, created_at, updated_at
        """

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, {"job_id": job_id, "new_state": new_state})
            row = cur.fetchone()

        if row is None:
            self._conn.rollback()
            return None

        self._conn.commit()
        return _row_to_record(row)

    def update_job_state_from_worker_result(self, job_id: str, new_state: str) -> bool:
        """
        Set ``state`` from a worker result event (no retry logic here).

        The result handler decides *which* ``new_state`` to apply (for example
        ``succeeded`` or ``failed``). This method only writes that state and
        bumps ``updated_at``. Returns True if the job row existed and was
        updated, False if ``job_id`` was not found.
        """
        sql = """
            UPDATE jobs
            SET state = %(new_state)s, updated_at = NOW()
            WHERE job_id = %(job_id)s
        """

        with self._conn.cursor() as cur:
            cur.execute(sql, {"job_id": job_id, "new_state": new_state})
            updated = cur.rowcount > 0

        self._conn.commit()
        return updated

    def schedule_retry_from_worker_result(self, job_id: str) -> bool:
        """
        Apply retry policy after a worker reports a retryable failure.

        - If the job is missing: return ``False``.
        - If ``retry_count < max_retries``: increment ``retry_count``, set
          ``retry_scheduled``, bump ``updated_at``, return ``True``.
        - Otherwise: set ``failed``, bump ``updated_at``, return ``True``.

        Backoff delay and re-enqueue to Kafka are **not** implemented here —
        this only updates Postgres so a future retry dispatcher can pick up
        ``retry_scheduled`` rows.
        """
        sql = """
            UPDATE jobs
            SET
                retry_count = CASE
                    WHEN retry_count < max_retries THEN retry_count + 1
                    ELSE retry_count
                END,
                state = CASE
                    WHEN retry_count < max_retries THEN %(retry_scheduled)s
                    ELSE %(failed)s
                END,
                updated_at = NOW()
            WHERE job_id = %(job_id)s
        """
        params = {
            "job_id": job_id,
            "retry_scheduled": JobState.RETRY_SCHEDULED.value,
            "failed": JobState.FAILED.value,
        }

        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            updated = cur.rowcount > 0

        self._conn.commit()
        return updated

    def delete_job(self, job_id: str) -> bool:
        """Delete a job by id. Returns True if a row was removed (handy for tests)."""
        sql = "DELETE FROM jobs WHERE job_id = %(job_id)s"

        with self._conn.cursor() as cur:
            cur.execute(sql, {"job_id": job_id})
            deleted = cur.rowcount > 0

        self._conn.commit()
        return deleted

    def list_schedulable_jobs(self, limit: int = 10) -> list[JobRecord]:
        """
        Return jobs ready for the scheduler to pick next.

        This is the first database-backed scheduling path: instead of an
        in-memory queue, the control plane asks Postgres which rows are waiting
        in ``queued`` state and orders them by policy (urgent first, then FIFO
        among equals). A future dispatch loop will call this, publish to Kafka,
        then mark winners as ``dispatched``.
        """
        sql = """
            SELECT
                job_id, tenant_id, priority, state, payload,
                retry_count, max_retries, created_at, updated_at
            FROM jobs
            WHERE state = %(queued_state)s
            ORDER BY priority DESC, created_at ASC
            LIMIT %(limit)s
        """

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                sql,
                {"queued_state": JobState.QUEUED.value, "limit": limit},
            )
            rows = cur.fetchall()

        self._conn.commit()
        return [_row_to_record(row) for row in rows]

    def mark_job_dispatched(self, job_id: str) -> JobRecord | None:
        """
        Move one job from ``queued`` to ``dispatched`` after it is selected.

        Fetching first makes the rule obvious: only jobs still waiting in the
        queue may be handed off. The UPDATE also checks ``state = queued`` so
        two schedulers cannot dispatch the same row if they race.
        """
        current = self.get_job(job_id)
        if current is None or current.state != JobState.QUEUED.value:
            return None

        sql = """
            UPDATE jobs
            SET state = %(new_state)s, updated_at = NOW()
            WHERE job_id = %(job_id)s AND state = %(queued_state)s
            RETURNING
                job_id, tenant_id, priority, state, payload,
                retry_count, max_retries, created_at, updated_at
        """
        params = {
            "job_id": job_id,
            "new_state": JobState.DISPATCHED.value,
            "queued_state": JobState.QUEUED.value,
        }

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()

        if row is None:
            self._conn.rollback()
            return None

        self._conn.commit()
        return _row_to_record(row)

    def claim_schedulable_jobs(self, limit: int = 10) -> list[JobRecord]:
        """
        Atomically pick schedulable jobs and mark them ``dispatched``.

        This is the safer path when **multiple scheduler instances** run ticks at
        the same time. ``SELECT ... FOR UPDATE SKIP LOCKED`` locks the rows we are
        about to claim; other schedulers **skip** locked rows instead of waiting,
        which prevents **duplicate dispatch** of the same job.

        Everything runs in **one transaction**: select-with-lock and update commit
        together, so no other session can see these rows as still ``queued`` after
        we claim them.

        Ordering matches ``list_schedulable_jobs``: ``priority DESC``, then
        ``created_at ASC``.
        """
        if limit <= 0:
            raise ValueError("limit must be a positive integer")

        # One statement: lock candidate rows, update them, return the new rows.
        sql = """
            UPDATE jobs
            SET state = %(dispatched_state)s, updated_at = NOW()
            WHERE job_id IN (
                SELECT job_id
                FROM jobs
                WHERE state = %(queued_state)s
                ORDER BY priority DESC, created_at ASC
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING
                job_id, tenant_id, priority, state, payload,
                retry_count, max_retries, created_at, updated_at
        """
        params = {
            "queued_state": JobState.QUEUED.value,
            "dispatched_state": JobState.DISPATCHED.value,
            "limit": limit,
        }

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        self._conn.commit()

        records = [_row_to_record(row) for row in rows]
        # RETURNING order is not guaranteed; sort to match scheduling policy.
        records.sort(key=lambda job: (-job.priority, job.created_at))
        return records
