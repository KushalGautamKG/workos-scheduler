"""
Prometheus text formatting for KernelQ control-plane metrics.

This module builds exposition-format strings (plain text) that Prometheus
or compatible scrapers can ingest. No third-party client library is required.
"""

from __future__ import annotations

from typing import Any


def format_job_state_counts_for_prometheus(job_state_counts: dict[str, int]) -> str:
    """
    Format Postgres job state counts as Prometheus text exposition.

    Each state becomes one gauge sample with a ``state`` label. States are
    emitted in alphabetical order for stable, diff-friendly output.

    Raises:
        ValueError: if a state is blank or a count is not a non-negative int.
    """
    lines = [
        "# HELP kernelq_jobs_by_state Number of jobs by durable lifecycle state.",
        "# TYPE kernelq_jobs_by_state gauge",
    ]

    for state in sorted(job_state_counts):
        count = job_state_counts[state]

        if not isinstance(state, str) or not state.strip():
            raise ValueError(f"state must be a non-blank string, got {state!r}")

        if not isinstance(count, int) or isinstance(count, bool):
            raise ValueError(f"count must be an int, got {count!r}")

        if count < 0:
            raise ValueError(f"count must be >= 0, got {count}")

        lines.append(f'kernelq_jobs_by_state{{state="{state}"}} {count}')

    return "\n".join(lines) + "\n"


def _validate_non_negative_number(name: str, value: Any) -> float:
    """Ensure a metric sample is a non-negative int or float."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number, got {value!r}")

    number = float(value)
    if number < 0:
        raise ValueError(f"{name} must be >= 0, got {value!r}")

    return number


def format_job_duration_metrics_for_prometheus(metrics: Any) -> str:
    """
    Format queue-wait percentile stats as Prometheus text exposition.

    Expects ``metrics`` with ``p50_queue_wait_seconds``, ``p95_queue_wait_seconds``,
    and ``p99_queue_wait_seconds`` (for example a ``JobDurationMetrics`` instance).
    """
    lines = [
        "# HELP kernelq_queue_wait_seconds Queue wait duration quantiles in seconds.",
        "# TYPE kernelq_queue_wait_seconds gauge",
    ]

    quantiles = (
        ("0.50", "p50_queue_wait_seconds"),
        ("0.95", "p95_queue_wait_seconds"),
        ("0.99", "p99_queue_wait_seconds"),
    )

    for quantile_label, field_name in quantiles:
        value = _validate_non_negative_number(
            field_name,
            getattr(metrics, field_name),
        )
        lines.append(
            f'kernelq_queue_wait_seconds{{quantile="{quantile_label}"}} {value}'
        )

    return "\n".join(lines) + "\n"
