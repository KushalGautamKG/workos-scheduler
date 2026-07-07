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


def _validate_non_negative_int(name: str, value: Any) -> int:
    """Ensure a counter sample is a non-negative int."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an int, got {value!r}")

    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")

    return value


def format_result_consumer_metrics(processed_messages: int, duplicate_messages: int) -> str:
    """
    Format result-consumer dedupe counters as Prometheus text exposition.

    Emits ``kernelq_result_consumer_processed_messages`` and
    ``kernelq_result_consumer_duplicate_messages`` counters in deterministic order.

    Raises:
        ValueError: if either count is not a non-negative int.
    """
    processed = _validate_non_negative_int("processed_messages", processed_messages)
    duplicates = _validate_non_negative_int("duplicate_messages", duplicate_messages)

    lines = [
        "# TYPE kernelq_result_consumer_processed_messages counter",
        f"kernelq_result_consumer_processed_messages {processed}",
        "",
        "# TYPE kernelq_result_consumer_duplicate_messages counter",
        f"kernelq_result_consumer_duplicate_messages {duplicates}",
    ]

    return "\n".join(lines) + "\n"
