"""
Worker result events consumed by the Python control plane.

Workers publish JSON to ``kernelq.jobs.results`` when a job attempt finishes.
The control plane will eventually read these events, validate them, and drive
Postgres state transitions (succeeded, retry, dead letter).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

# Kafka topic where Go workers publish execution outcomes.
RESULT_TOPIC = "kernelq.jobs.results"

# Only this event_type is allowed on RESULT_TOPIC (matches the Go worker contract).
WORKER_RESULT_EVENT_TYPE = "job.result"

# Allowed ``status`` values on WorkerResultEvent (matches worker ExecutionStatus).
ALLOWED_STATUSES = frozenset(
    {
        "succeeded",
        "retryable_failure",
        "terminal_failure",
    }
)


@dataclass
class WorkerResultEvent:
    """
    On-wire shape for a job execution outcome reported by a worker.

    Field names match the JSON keys workers publish to Kafka.
    """

    event_type: str
    job_id: str
    status: str
    message: str
    worker: str

    def validate(self) -> None:
        """
        Check required fields before we trust this event for Postgres updates.

        Raises ValueError if anything is invalid. ``message`` may be blank.
        """
        if self.event_type != WORKER_RESULT_EVENT_TYPE:
            raise ValueError(
                f"event_type must be {WORKER_RESULT_EVENT_TYPE!r}, got {self.event_type!r}"
            )

        if not self.job_id.strip():
            raise ValueError("job_id must not be blank")

        if self.status not in ALLOWED_STATUSES:
            allowed = ", ".join(sorted(ALLOWED_STATUSES))
            raise ValueError(f"status must be one of: {allowed}, got {self.status!r}")

        if not self.worker.strip():
            raise ValueError("worker must not be blank")

    def to_dict(self) -> dict:
        """Convert to a plain dict with JSON field names."""
        return {
            "event_type": self.event_type,
            "job_id": self.job_id,
            "status": self.status,
            "message": self.message,
            "worker": self.worker,
        }

    def to_json(self) -> str:
        """Validate, then encode as a JSON string."""
        self.validate()
        return json.dumps(self.to_dict())


def parse_worker_result_event(data: bytes | str) -> WorkerResultEvent:
    """
    Parse and validate a worker result event from Kafka (or a test fixture).

    Args:
        data: Raw JSON as ``bytes`` or ``str``.

    Returns:
        A validated WorkerResultEvent.

    Raises:
        ValueError: Invalid JSON or invalid event fields.
    """
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("result event must be valid UTF-8") from exc
    else:
        text = data

    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid JSON for worker result event") from exc

    if not isinstance(payload, dict):
        raise ValueError("worker result event must be a JSON object")

    event = WorkerResultEvent(
        event_type=str(payload.get("event_type", "")),
        job_id=str(payload.get("job_id", "")),
        status=str(payload.get("status", "")),
        message=str(payload.get("message", "")),
        worker=str(payload.get("worker", "")),
    )
    event.validate()
    return event
