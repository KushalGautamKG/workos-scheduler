"""
Persist and load KernelQ jobs in PostgreSQL.

The API layer should not embed SQL strings everywhere. This small repository
keeps INSERT/SELECT/UPDATE/DELETE in one place and uses parameterized queries
so values are never pasted into SQL as raw strings (safer and clearer).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from psycopg import Connection
from psycopg.rows import dict_row
from psycopg.types.json import Json

from control_plane.kernelq.job_state import JobState

# Default wait before a retry_scheduled job becomes eligible for requeue.
DEFAULT_RETRY_DELAY_SECONDS = 60


@dataclass
class ScheduleRetryResult:
    """
    Outcome of ``schedule_retry_from_worker_result`` for one job.

    - ``outcome`` is ``"scheduled"`` when retries remain and the job waits on
      ``retry_after`` in ``retry_scheduled``.
    - ``outcome`` is ``"exhausted"`` when ``retry_count >= max_retries`` and the
      job moves to ``dead_lettered`` (max retry budget used up).
    """

    outcome: str
    job_id: str
    state: str
    retry_count: int
    max_retries: int
    retry_after: int | None = None


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

    def schedule_retry_from_worker_result(
        self,
        job_id: str,
        retry_delay_seconds: int = DEFAULT_RETRY_DELAY_SECONDS,
    ) -> ScheduleRetryResult | None:
        """
        Apply retry policy after a worker reports a retryable failure.

        - If the job is missing: return ``None``.
        - If ``retry_count < max_retries``: increment ``retry_count``, set
          ``retry_scheduled``, set ``retry_after = now + retry_delay_seconds``,
          bump ``updated_at``; return outcome ``"scheduled"``.
        - If ``retry_count >= max_retries`` (**max retry exhaustion**): set
          ``dead_lettered``, bump ``updated_at``; return outcome ``"exhausted"``.
          The job will not be auto-retried — operators inspect dead-letter state.

        Re-enqueue to ``queued`` is handled separately by ``requeue_due_retries``.
        """
        now = int(time.time())
        retry_after = now + retry_delay_seconds

        sql = """
            UPDATE jobs
            SET
                retry_count = CASE
                    WHEN retry_count < max_retries THEN retry_count + 1
                    ELSE retry_count
                END,
                state = CASE
                    WHEN retry_count < max_retries THEN %(retry_scheduled)s
                    ELSE %(dead_lettered)s
                END,
                retry_after = CASE
                    WHEN retry_count < max_retries THEN %(retry_after)s
                    ELSE retry_after
                END,
                updated_at = NOW()
            WHERE job_id = %(job_id)s
            RETURNING job_id, retry_count, max_retries, state, retry_after
        """
        params = {
            "job_id": job_id,
            "retry_scheduled": JobState.RETRY_SCHEDULED.value,
            "dead_lettered": JobState.DEAD_LETTERED.value,
            "retry_after": retry_after,
        }

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()

        if row is None:
            self._conn.rollback()
            return None

        self._conn.commit()

        if row["state"] == JobState.RETRY_SCHEDULED.value:
            outcome = "scheduled"
        else:
            outcome = "exhausted"

        return ScheduleRetryResult(
            outcome=outcome,
            job_id=row["job_id"],
            state=row["state"],
            retry_count=row["retry_count"],
            max_retries=row["max_retries"],
            retry_after=row.get("retry_after"),
        )

    def requeue_due_retries(self, now: int, limit: int = 100) -> list[str]:
        """
        Move due retry jobs back into the normal scheduling queue.

        Finds rows where ``state`` is ``retry_scheduled`` and ``retry_after <= now``,
        updates them to ``queued``, bumps ``updated_at``, and returns the
        ``job_id`` values that were requeued (at most ``limit``).

        Ordering: earliest ``retry_after`` first, then oldest ``created_at`` (FIFO
        among jobs that become due at the same time).

        ``now`` is a Unix timestamp (seconds). A future retry scanner passes the
        current time on each pass.
        """
        if limit <= 0:
            raise ValueError("limit must be a positive integer")

        # One transaction: lock due retry rows, move them to queued, return ids.
        sql = """
            UPDATE jobs
            SET state = %(queued_state)s, updated_at = NOW()
            WHERE job_id IN (
                SELECT job_id
                FROM jobs
                WHERE state = %(retry_scheduled_state)s
                  AND retry_after <= %(now)s
                ORDER BY retry_after ASC, created_at ASC
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING job_id, retry_after, created_at
        """
        params = {
            "queued_state": JobState.QUEUED.value,
            "retry_scheduled_state": JobState.RETRY_SCHEDULED.value,
            "now": now,
            "limit": limit,
        }

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        self._conn.commit()

        # RETURNING order is not guaranteed; sort to match selection policy.
        rows.sort(key=lambda row: (row["retry_after"], row["created_at"]))
        return [row["job_id"] for row in rows]

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

    def list_dead_lettered_jobs(self, limit: int = 20) -> list[dict[str, Any]]:
        """
        Return recent dead-lettered jobs for operator inspection.

        Dead-lettered rows are terminal — they will not be retried automatically.
        Results are ordered by ``updated_at`` descending so the newest failures
        appear first.
        """
        if limit <= 0:
            raise ValueError("limit must be a positive integer")

        sql = """
            SELECT
                job_id, tenant_id, priority, state, payload,
                retry_count, max_retries, created_at, updated_at
            FROM jobs
            WHERE state = %(dead_lettered_state)s
            ORDER BY updated_at DESC
            LIMIT %(limit)s
        """

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                sql,
                {
                    "dead_lettered_state": JobState.DEAD_LETTERED.value,
                    "limit": limit,
                },
            )
            rows = cur.fetchall()

        self._conn.commit()

        # Return plain dicts (not JobRecord) for easy JSON/API use in inspection tooling.
        result: list[dict[str, Any]] = []
        for row in rows:
            raw_payload = row.get("payload")
            if isinstance(raw_payload, dict):
                payload = dict(raw_payload)
            else:
                payload = {}

            result.append(
                {
                    "job_id": row["job_id"],
                    "tenant_id": row["tenant_id"],
                    "priority": row["priority"],
                    "state": row["state"],
                    "retry_count": row["retry_count"],
                    "max_retries": row["max_retries"],
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "payload": payload,
                }
            )

        return result

    def count_jobs_by_state(self) -> dict[str, int]:
        """
        Return how many jobs exist in each ``state``.

        Only states that appear in the ``jobs`` table are included (no zero
        counts for missing states).
        """
        sql = """
            SELECT state, COUNT(*) AS count
            FROM jobs
            GROUP BY state
        """

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql)
            rows = cur.fetchall()

        self._conn.commit()
        return {row["state"]: int(row["count"]) for row in rows}

    def list_jobs(self, limit: int = 100_000) -> list[JobRecord]:
        """
        Return job rows for metrics snapshots and inspection.

        Use a ``limit`` cap on large local datasets; ordering is oldest first.
        """
        if limit <= 0:
            raise ValueError("limit must be a positive integer")

        sql = """
            SELECT
                job_id, tenant_id, priority, state, payload,
                retry_count, max_retries, created_at, updated_at
            FROM jobs
            ORDER BY created_at ASC
            LIMIT %(limit)s
        """

        with self._conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, {"limit": limit})
            rows = cur.fetchall()

        self._conn.commit()
        return [_row_to_record(row) for row in rows]

    def requeue_dead_lettered_job(self, job_id: str) -> bool:
        """
        Manually move one dead-lettered job back to the normal queue.

        Operator-driven replay only — not automatic retry. The UPDATE runs only
        when the row is currently ``dead_lettered``.

        - Sets ``state`` to ``queued`` and bumps ``updated_at``.
        - Clears ``retry_after`` (if present) so the job is not treated as a
          delayed retry waiting on ``RetryScanner``.
        - Does **not** reset ``retry_count`` — exhaustion history stays visible.

        Returns True if a row was updated, False if ``job_id`` is missing or
        the job is not in ``dead_lettered`` state.
        """
        sql = """
            UPDATE jobs
            SET
                state = %(queued_state)s,
                retry_after = NULL,
                updated_at = NOW()
            WHERE job_id = %(job_id)s
              AND state = %(dead_lettered_state)s
        """
        params = {
            "job_id": job_id,
            "queued_state": JobState.QUEUED.value,
            "dead_lettered_state": JobState.DEAD_LETTERED.value,
        }

        with self._conn.cursor() as cur:
            cur.execute(sql, params)
            updated = cur.rowcount > 0

        self._conn.commit()
        return updated

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
