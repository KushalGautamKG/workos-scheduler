"""Unit tests for RetryScanner (fake repository, no Postgres)."""

from __future__ import annotations

import pytest

from control_plane.kernelq.retry_scanner import RetryScanner


class FakeRepository:
    """Records requeue calls and returns a configurable job id list."""

    def __init__(
        self,
        *,
        requeued_job_ids: list[str] | None = None,
        raise_error: Exception | None = None,
    ) -> None:
        self.requeued_job_ids = (
            list(requeued_job_ids) if requeued_job_ids is not None else []
        )
        self.raise_error = raise_error
        self.calls: list[tuple[int, int]] = []

    def requeue_due_retries(self, now: int, limit: int) -> list[str]:
        self.calls.append((now, limit))
        if self.raise_error is not None:
            raise self.raise_error
        return list(self.requeued_job_ids)


def test_run_once_requeues_due_jobs_and_returns_count():
    repo = FakeRepository(requeued_job_ids=["job-a", "job-b"])
    scanner = RetryScanner(repo, max_jobs_per_scan=50)

    result = scanner.run_once(now=1_700_000_000)

    assert result.requeued_count == 2
    assert result.requeued_job_ids == ["job-a", "job-b"]
    assert result.scanned_at == 1_700_000_000
    assert result.errors == []


def test_run_once_passes_now_and_limit_to_repository():
    repo = FakeRepository(requeued_job_ids=["job-1"])
    scanner = RetryScanner(repo, max_jobs_per_scan=7)

    scanner.run_once(now=1_800_000_000)

    assert repo.calls == [(1_800_000_000, 7)]


def test_run_once_uses_current_time_when_now_is_none(monkeypatch):
    repo = FakeRepository(requeued_job_ids=[])
    scanner = RetryScanner(repo)

    monkeypatch.setattr("control_plane.kernelq.retry_scanner.time.time", lambda: 1_900_000_000.9)

    result = scanner.run_once()

    assert result.scanned_at == 1_900_000_000
    assert repo.calls == [(1_900_000_000, 100)]


def test_run_once_captures_repository_exception_in_errors():
    repo = FakeRepository(raise_error=RuntimeError("db down"))
    scanner = RetryScanner(repo)

    result = scanner.run_once(now=1_700_000_001)

    assert result.requeued_count == 0
    assert result.requeued_job_ids == []
    assert len(result.errors) == 1
    assert "requeue_due_retries" in result.errors[0]
    assert "RuntimeError" in result.errors[0]
    assert "db down" in result.errors[0]


def test_run_once_missing_repository_returns_error():
    scanner = RetryScanner(None)

    result = scanner.run_once(now=1_700_000_002)

    assert result.requeued_count == 0
    assert result.requeued_job_ids == []
    assert result.errors == ["repository must not be None"]
