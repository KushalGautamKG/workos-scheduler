"""
Canonical idempotency key builders for KernelQ duplicate suppression.

Each helper returns the **logical key segment** passed to ``IdempotencyStore.try_claim``
(``RedisIdempotencyStore`` adds its own namespace prefix, e.g. ``kernelq:idempotency:``).

Use separate prefixes per pipeline stage so dispatch, execution, and result dedupe
do not block each other for the same ``job_id``.

This module uses only the Python standard library.
"""

from __future__ import annotations


def _validate_job_id(job_id: str) -> str:
    """Reject blank job ids; return stripped value for callers that need it."""
    if not isinstance(job_id, str):
        raise ValueError("job_id must be a non-empty string")
    stripped = job_id.strip()
    if not stripped:
        raise ValueError("job_id must be a non-empty string")
    return stripped


def _validate_attempt(attempt: int) -> int:
    """Retry generation must be a non-negative integer (0 = first run)."""
    if not isinstance(attempt, int):
        raise ValueError("attempt must be a non-negative integer")
    if attempt < 0:
        raise ValueError("attempt must be a non-negative integer")
    return attempt


def _validate_event_id(event_id: str) -> str:
    """Reject blank opaque event ids."""
    if not isinstance(event_id, str):
        raise ValueError("event_id must be a non-empty string")
    stripped = event_id.strip()
    if not stripped:
        raise ValueError("event_id must be a non-empty string")
    return stripped


def worker_result_key(job_id: str, attempt: int) -> str:
    """
    Key for deduping **result consumer → Postgres** updates.

    Example: ``worker-result:job-abc:0``
    """
    job = _validate_job_id(job_id)
    gen = _validate_attempt(attempt)
    return f"worker-result:{job}:{gen}"


def dispatch_key(job_id: str, attempt: int) -> str:
    """
    Key for deduping **scheduler dispatch publish** handoffs.

    Example: ``dispatch:job-abc:0``
    """
    job = _validate_job_id(job_id)
    gen = _validate_attempt(attempt)
    return f"dispatch:{job}:{gen}"


def execution_key(job_id: str, attempt: int) -> str:
    """
    Key for deduping **worker execution intake** on ``kernelq.jobs.dispatch``.

    Example: ``execution:job-abc:0``
    """
    job = _validate_job_id(job_id)
    gen = _validate_attempt(attempt)
    return f"execution:{job}:{gen}"


def event_key(event_id: str) -> str:
    """
    Key for generic opaque events (outbox, API idempotency, audit).

    Example: ``event:evt-7f3a…``
    """
    event = _validate_event_id(event_id)
    return f"event:{event}"
