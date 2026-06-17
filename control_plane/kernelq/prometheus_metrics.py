"""
Prometheus text formatting for KernelQ control-plane metrics.

This module builds exposition-format strings (plain text) that Prometheus
or compatible scrapers can ingest. No third-party client library is required.
"""

from __future__ import annotations


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
