"""
Structured log line formatting for KernelQ control-plane scripts.

Produces single-line key=value output that is easy to grep and parse.
Uses only the Python standard library.
"""

from __future__ import annotations

import json
from typing import Any


def _format_log_value(value: Any) -> str:
    """
    Convert one field value to its log representation.

    Rules (in order):
    - None -> null
    - bool -> true / false (lowercase)
    - str with spaces -> JSON-quoted string
    - str without spaces -> printed as-is
    - list / dict -> JSON with sorted keys
    - everything else -> str(value)
    """
    if value is None:
        return "null"

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, str):
        if " " in value:
            return json.dumps(value)
        return value

    if isinstance(value, (list, dict)):
        return json.dumps(value, sort_keys=True)

    return str(value)


def format_log_event(event: str, **fields: Any) -> str:
    """
    Build one structured log line: event=<name> key=value ...

    Field keys are sorted alphabetically so output is stable across runs.
    Raises ValueError if event is blank (empty or whitespace only).
    """
    if not isinstance(event, str) or not event.strip():
        raise ValueError("event must be a non-blank string")

    parts = [f"event={_format_log_value(event)}"]

    for key in sorted(fields):
        parts.append(f"{key}={_format_log_value(fields[key])}")

    return " ".join(parts)
