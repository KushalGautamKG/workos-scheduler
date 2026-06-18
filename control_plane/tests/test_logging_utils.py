"""Tests for structured log line formatting."""

from __future__ import annotations

import pytest

from control_plane.kernelq.logging_utils import format_log_event


def test_event_appears_first() -> None:
    line = format_log_event("tick_complete", job_id="abc", count=1)

    assert line.startswith("event=tick_complete")


def test_fields_are_sorted_alphabetically() -> None:
    line = format_log_event("tick_complete", zebra="z", alpha="a", middle="m")

    assert line == "event=tick_complete alpha=a middle=m zebra=z"


def test_bool_values_are_lowercase() -> None:
    line = format_log_event("tick_complete", ok=True, failed=False)

    assert "ok=true" in line
    assert "failed=false" in line


def test_none_prints_as_null() -> None:
    line = format_log_event("tick_complete", err=None)

    assert "err=null" in line


def test_strings_with_spaces_are_json_quoted() -> None:
    line = format_log_event("tick_complete", msg="hello world")

    assert 'msg="hello world"' in line


def test_lists_and_dicts_are_json_encoded() -> None:
    line = format_log_event(
        "tick_complete",
        tags=["b", "a"],
        meta={"z": 1, "a": 2},
    )

    assert 'tags=["b", "a"]' in line
    assert 'meta={"a": 2, "z": 1}' in line


def test_blank_event_raises_value_error() -> None:
    with pytest.raises(ValueError, match="non-blank"):
        format_log_event("   ")


def test_output_is_single_line() -> None:
    line = format_log_event(
        "tick_complete",
        msg="has spaces",
        ok=True,
        tags=[1, 2],
    )

    assert "\n" not in line
