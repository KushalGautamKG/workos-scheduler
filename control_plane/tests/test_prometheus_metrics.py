"""Tests for Prometheus text formatting helpers."""

from __future__ import annotations

import pytest

from control_plane.kernelq.job_metrics import JobDurationMetrics
from control_plane.kernelq.prometheus_metrics import (
    format_job_duration_metrics_for_prometheus,
    format_job_state_counts_for_prometheus,
    format_result_consumer_metrics,
)


def test_formatter_includes_help_and_type_lines() -> None:
    output = format_job_state_counts_for_prometheus({"queued": 1})

    assert "# HELP kernelq_jobs_by_state Number of jobs by durable lifecycle state." in output
    assert "# TYPE kernelq_jobs_by_state gauge" in output


def test_formatter_outputs_one_metric_line_per_state() -> None:
    output = format_job_state_counts_for_prometheus(
        {"queued": 2, "succeeded": 5, "dead_lettered": 1}
    )

    metric_lines = [
        line for line in output.splitlines() if line.startswith("kernelq_jobs_by_state{")
    ]

    assert len(metric_lines) == 3


def test_formatter_sorts_states_alphabetically() -> None:
    output = format_job_state_counts_for_prometheus(
        {"queued": 1, "dead_lettered": 2, "succeeded": 3}
    )

    metric_lines = [
        line for line in output.splitlines() if line.startswith("kernelq_jobs_by_state{")
    ]

    assert metric_lines == [
        'kernelq_jobs_by_state{state="dead_lettered"} 2',
        'kernelq_jobs_by_state{state="queued"} 1',
        'kernelq_jobs_by_state{state="succeeded"} 3',
    ]


def test_formatter_output_ends_with_newline() -> None:
    output = format_job_state_counts_for_prometheus({"queued": 0})

    assert output.endswith("\n")


def test_formatter_blank_state_raises_value_error() -> None:
    with pytest.raises(ValueError, match="non-blank"):
        format_job_state_counts_for_prometheus({"": 1})

    with pytest.raises(ValueError, match="non-blank"):
        format_job_state_counts_for_prometheus({"   ": 1})


def test_formatter_negative_count_raises_value_error() -> None:
    with pytest.raises(ValueError, match=">= 0"):
        format_job_state_counts_for_prometheus({"queued": -1})


def test_formatter_non_int_count_raises_value_error() -> None:
    with pytest.raises(ValueError, match="count must be an int"):
        format_job_state_counts_for_prometheus({"queued": 1.5})  # type: ignore[dict-item]

    with pytest.raises(ValueError, match="count must be an int"):
        format_job_state_counts_for_prometheus({"queued": True})  # type: ignore[dict-item]


def test_duration_formatter_includes_help_and_type_lines() -> None:
    metrics = JobDurationMetrics(
        completed_jobs_count=1,
        average_queue_wait_seconds=10.0,
        average_completion_seconds=50.0,
        p50_queue_wait_seconds=10.0,
        p95_queue_wait_seconds=20.0,
        p99_queue_wait_seconds=30.0,
    )

    output = format_job_duration_metrics_for_prometheus(metrics)

    assert "# HELP kernelq_queue_wait_seconds Queue wait duration quantiles in seconds." in output
    assert "# TYPE kernelq_queue_wait_seconds gauge" in output


def test_duration_formatter_outputs_quantile_gauges() -> None:
    metrics = JobDurationMetrics(
        completed_jobs_count=3,
        average_queue_wait_seconds=15.0,
        average_completion_seconds=60.0,
        p50_queue_wait_seconds=10.0,
        p95_queue_wait_seconds=25.0,
        p99_queue_wait_seconds=40.0,
    )

    output = format_job_duration_metrics_for_prometheus(metrics)

    assert output.splitlines()[-3:] == [
        'kernelq_queue_wait_seconds{quantile="0.50"} 10.0',
        'kernelq_queue_wait_seconds{quantile="0.95"} 25.0',
        'kernelq_queue_wait_seconds{quantile="0.99"} 40.0',
    ]


def test_duration_formatter_output_ends_with_newline() -> None:
    metrics = JobDurationMetrics(
        completed_jobs_count=0,
        average_queue_wait_seconds=0.0,
        average_completion_seconds=0.0,
        p50_queue_wait_seconds=0.0,
        p95_queue_wait_seconds=0.0,
        p99_queue_wait_seconds=0.0,
    )

    output = format_job_duration_metrics_for_prometheus(metrics)

    assert output.endswith("\n")


def test_duration_formatter_negative_value_raises_value_error() -> None:
    metrics = JobDurationMetrics(
        completed_jobs_count=1,
        average_queue_wait_seconds=0.0,
        average_completion_seconds=0.0,
        p50_queue_wait_seconds=-1.0,
        p95_queue_wait_seconds=0.0,
        p99_queue_wait_seconds=0.0,
    )

    with pytest.raises(ValueError, match=">= 0"):
        format_job_duration_metrics_for_prometheus(metrics)


def test_result_consumer_metrics_include_processed_counter() -> None:
    output = format_result_consumer_metrics(processed_messages=3, duplicate_messages=1)

    assert "kernelq_result_consumer_processed_messages 3" in output
    assert "# TYPE kernelq_result_consumer_processed_messages counter" in output


def test_result_consumer_metrics_include_duplicate_counter() -> None:
    output = format_result_consumer_metrics(processed_messages=3, duplicate_messages=1)

    assert "kernelq_result_consumer_duplicate_messages 1" in output
    assert "# TYPE kernelq_result_consumer_duplicate_messages counter" in output


def test_result_consumer_metrics_zero_values_allowed() -> None:
    output = format_result_consumer_metrics(processed_messages=0, duplicate_messages=0)

    assert "kernelq_result_consumer_processed_messages 0" in output
    assert "kernelq_result_consumer_duplicate_messages 0" in output


def test_result_consumer_metrics_negative_processed_raises_value_error() -> None:
    with pytest.raises(ValueError, match="processed_messages must be >= 0"):
        format_result_consumer_metrics(processed_messages=-1, duplicate_messages=0)


def test_result_consumer_metrics_negative_duplicate_raises_value_error() -> None:
    with pytest.raises(ValueError, match="duplicate_messages must be >= 0"):
        format_result_consumer_metrics(processed_messages=0, duplicate_messages=-1)


def test_result_consumer_metrics_deterministic_output_order() -> None:
    output = format_result_consumer_metrics(processed_messages=2, duplicate_messages=5)

    assert output.splitlines() == [
        "# TYPE kernelq_result_consumer_processed_messages counter",
        "kernelq_result_consumer_processed_messages 2",
        "",
        "# TYPE kernelq_result_consumer_duplicate_messages counter",
        "kernelq_result_consumer_duplicate_messages 5",
    ]
