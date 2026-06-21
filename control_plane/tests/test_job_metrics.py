"""Tests for job duration metrics."""

from __future__ import annotations

from types import SimpleNamespace

from control_plane.kernelq.job_metrics import compute_job_duration_metrics


def _job(
    state: str,
    *,
    created_at: int = 0,
    dispatched_at: int | None = None,
    updated_at: int | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        state=state,
        created_at=created_at,
        dispatched_at=dispatched_at,
        updated_at=updated_at,
    )


def test_empty_list_returns_zeros() -> None:
    metrics = compute_job_duration_metrics([])

    assert metrics.completed_jobs_count == 0
    assert metrics.average_queue_wait_seconds == 0.0
    assert metrics.average_completion_seconds == 0.0
    assert metrics.p50_queue_wait_seconds == 0.0
    assert metrics.p95_queue_wait_seconds == 0.0
    assert metrics.p99_queue_wait_seconds == 0.0


def test_one_completed_job() -> None:
    metrics = compute_job_duration_metrics(
        [_job("succeeded", created_at=100, dispatched_at=110, updated_at=150)]
    )

    assert metrics.completed_jobs_count == 1
    assert metrics.average_queue_wait_seconds > 0
    assert metrics.average_queue_wait_seconds == 10.0
    assert metrics.average_completion_seconds == 50.0


def test_multiple_completed_jobs() -> None:
    metrics = compute_job_duration_metrics(
        [
            _job("succeeded", created_at=0, dispatched_at=10, updated_at=50),
            _job("succeeded", created_at=0, dispatched_at=20, updated_at=100),
        ]
    )

    assert metrics.completed_jobs_count == 2
    assert metrics.average_queue_wait_seconds == 15.0
    assert metrics.average_completion_seconds == 75.0


def test_queued_jobs_ignored_for_completion_metric() -> None:
    metrics = compute_job_duration_metrics(
        [
            _job("queued", created_at=0, dispatched_at=5, updated_at=99),
            _job("succeeded", created_at=0, dispatched_at=10, updated_at=40),
        ]
    )

    assert metrics.completed_jobs_count == 1
    assert metrics.average_queue_wait_seconds == 10.0
    assert metrics.average_completion_seconds == 40.0


def test_missing_timestamps_ignored() -> None:
    metrics = compute_job_duration_metrics(
        [
            # No dispatched_at -> skipped for queue wait, still counts for completion.
            _job("succeeded", created_at=100, updated_at=200),
            # No updated_at -> skipped for completion, still counts for queue wait.
            _job("failed", created_at=0, dispatched_at=30),
        ]
    )

    assert metrics.completed_jobs_count == 2
    assert metrics.average_queue_wait_seconds == 30.0
    assert metrics.average_completion_seconds == 100.0


def test_queue_wait_percentiles_zero_when_no_queue_waits() -> None:
    metrics = compute_job_duration_metrics(
        [_job("succeeded", created_at=100, updated_at=200)]
    )

    assert metrics.p50_queue_wait_seconds == 0.0
    assert metrics.p95_queue_wait_seconds == 0.0
    assert metrics.p99_queue_wait_seconds == 0.0


def test_queue_wait_percentiles_single_value() -> None:
    metrics = compute_job_duration_metrics(
        [_job("succeeded", created_at=100, dispatched_at=107, updated_at=200)]
    )

    assert metrics.p50_queue_wait_seconds == 7.0
    assert metrics.p95_queue_wait_seconds == 7.0
    assert metrics.p99_queue_wait_seconds == 7.0


def test_queue_wait_percentiles_deterministic_seed_waits() -> None:
    """Queue waits 1, 2, 3, 5, 10 — nearest-rank percentiles, no Postgres."""
    waits = [1, 2, 3, 5, 10]
    metrics = compute_job_duration_metrics(
        [
            _job("succeeded", created_at=0, dispatched_at=wait, updated_at=wait + 1)
            for wait in waits
        ]
    )

    p50 = metrics.p50_queue_wait_seconds
    p95 = metrics.p95_queue_wait_seconds
    p99 = metrics.p99_queue_wait_seconds

    assert p50 > 0
    assert p95 >= p50
    assert p99 >= p95
    assert p50 == 3.0
    assert p95 == 10.0
    assert p99 == 10.0


def test_queue_wait_percentiles_ordered_for_multiple_values() -> None:
    waits = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    metrics = compute_job_duration_metrics(
        [
            _job("succeeded", created_at=0, dispatched_at=wait, updated_at=wait + 1)
            for wait in waits
        ]
    )

    p50 = metrics.p50_queue_wait_seconds
    p95 = metrics.p95_queue_wait_seconds
    p99 = metrics.p99_queue_wait_seconds

    assert p50 <= p95 <= p99
    assert p50 == 50.0
    assert p95 == 100.0
    assert p99 == 100.0


def test_queue_wait_percentiles_missing_dispatched_at_ignored() -> None:
    metrics = compute_job_duration_metrics(
        [
            _job("succeeded", created_at=0, dispatched_at=5, updated_at=10),
            _job("succeeded", created_at=0, updated_at=10),
        ]
    )

    assert metrics.p50_queue_wait_seconds == 5.0
    assert metrics.p95_queue_wait_seconds == 5.0
    assert metrics.p99_queue_wait_seconds == 5.0


def test_queue_wait_percentiles_negative_ignored() -> None:
    metrics = compute_job_duration_metrics(
        [
            _job("succeeded", created_at=100, dispatched_at=90, updated_at=150),
            _job("succeeded", created_at=0, dispatched_at=20, updated_at=80),
        ]
    )

    assert metrics.p50_queue_wait_seconds == 20.0
    assert metrics.p95_queue_wait_seconds == 20.0
    assert metrics.p99_queue_wait_seconds == 20.0


def test_negative_queue_wait_ignored() -> None:
    metrics = compute_job_duration_metrics(
        [
            # dispatched before created_at — invalid, skipped for queue wait.
            _job("succeeded", created_at=100, dispatched_at=90, updated_at=150),
            _job("succeeded", created_at=0, dispatched_at=20, updated_at=80),
        ]
    )

    assert metrics.completed_jobs_count == 2
    assert metrics.average_queue_wait_seconds > 0
    assert metrics.average_queue_wait_seconds == 20.0
    assert metrics.average_completion_seconds == (50.0 + 80.0) / 2


def test_dead_lettered_included() -> None:
    metrics = compute_job_duration_metrics(
        [_job("dead_lettered", created_at=0, dispatched_at=5, updated_at=25)]
    )

    assert metrics.completed_jobs_count == 1
    assert metrics.average_queue_wait_seconds == 5.0
    assert metrics.average_completion_seconds == 25.0


def test_failed_included() -> None:
    metrics = compute_job_duration_metrics(
        [_job("failed", created_at=10, dispatched_at=15, updated_at=60)]
    )

    assert metrics.completed_jobs_count == 1
    assert metrics.average_queue_wait_seconds == 5.0
    assert metrics.average_completion_seconds == 50.0
