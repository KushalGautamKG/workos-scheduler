"""
One retry scan pass: move due retry_scheduled jobs back to queued.

The retry scanner is the control-plane side of retry requeue — it asks Postgres
which jobs finished their wait (``retry_after <= now``) and moves them to
``queued`` so the normal scheduler tick can dispatch them again.

No backoff tuning or Kafka publish here yet; this module only runs one scan.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class RetryScannerResult:
    """
    Summary of a single retry scan pass.

    - scanned_at: Unix timestamp (seconds) used for the scan
    - requeued_count: how many jobs moved to ``queued``
    - requeued_job_ids: ids that were requeued (in selection order)
    - errors: human-readable problems (missing repository, DB failures, etc.)
    """

    scanned_at: int
    requeued_count: int
    requeued_job_ids: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class RetryScanner:
    """
    Run one database-backed retry scan using ``JobRepository``.

    Typical use (from a loop or cron in the control plane):

        scanner = RetryScanner(repository, max_jobs_per_scan=100)
        result = scanner.run_once()
        # inspect result.requeued_count, result.requeued_job_ids, result.errors
    """

    def __init__(self, repository, max_jobs_per_scan: int = 100) -> None:
        if max_jobs_per_scan <= 0:
            raise ValueError("max_jobs_per_scan must be a positive integer")

        self._repository = repository
        self._max_jobs_per_scan = max_jobs_per_scan

    def run_once(self, now: int | None = None) -> RetryScannerResult:
        """
        Execute one retry scan.

        1. Resolve ``now`` (default: current Unix time).
        2. ``requeue_due_retries`` — move due ``retry_scheduled`` rows to ``queued``.
        3. Return counts and ids (or errors if the repository call fails).
        """
        scanned_at = int(time.time()) if now is None else now

        if self._repository is None:
            return RetryScannerResult(
                scanned_at=scanned_at,
                requeued_count=0,
                requeued_job_ids=[],
                errors=["repository must not be None"],
            )

        try:
            requeued_job_ids = self._repository.requeue_due_retries(
                now=scanned_at,
                limit=self._max_jobs_per_scan,
            )
        except Exception as exc:
            return RetryScannerResult(
                scanned_at=scanned_at,
                requeued_count=0,
                requeued_job_ids=[],
                errors=[f"requeue_due_retries: {type(exc).__name__}: {exc}"],
            )

        return RetryScannerResult(
            scanned_at=scanned_at,
            requeued_count=len(requeued_job_ids),
            requeued_job_ids=list(requeued_job_ids),
            errors=[],
        )
