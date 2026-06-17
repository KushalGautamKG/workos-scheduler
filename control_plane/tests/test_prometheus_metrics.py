"""Tests for Prometheus text formatting helpers."""

from __future__ import annotations

import pytest

from control_plane.kernelq.prometheus_metrics import format_job_state_counts_for_prometheus


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
