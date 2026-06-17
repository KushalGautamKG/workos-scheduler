"""
FastAPI control-plane API for KernelQ.

Job rows are stored in PostgreSQL through JobRepository. Scheduling queues and
Kafka dispatch are not wired here yet—this module focuses on HTTP + durable state.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Path, Response
from psycopg import Error as PsycopgError
from psycopg.errors import UniqueViolation
from pydantic import BaseModel, Field

from control_plane.kernelq.db import connect
from control_plane.kernelq.enqueue_result import EnqueueStatus
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.job_state import JobState, can_transition, explain_transition
from control_plane.kernelq.prometheus_metrics import format_job_state_counts_for_prometheus
from control_plane.kernelq.scheduler_metrics import SchedulerMetrics


# In-process metrics only (not persisted to Postgres).
metrics = SchedulerMetrics()


def get_repository() -> JobRepository:
    """
    Open a new database connection and wrap it in a JobRepository.

    Callers should close the connection when finished (see ``_close_repository``).
    Connection pooling can be added later.
    """
    conn = connect()
    return JobRepository(conn)


def _close_repository(repo: JobRepository) -> None:
    """Close the underlying psycopg connection for a repository."""
    repo._conn.close()


def _parse_state(state_value: str) -> JobState:
    """
    Convert a database state string to JobState.

    Values in Postgres use lowercase strings (for example ``queued``, ``failed``)
    matching JobState enum values—not uppercase enum names.
    """
    try:
        return JobState(state_value)
    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Job has unknown state {state_value!r} in database",
        ) from exc


def _record_to_response(record: Any) -> "JobResponse":
    """Map a repository JobRecord to the public API response model."""
    created_at = record.created_at
    updated_at = record.updated_at
    if isinstance(created_at, datetime):
        created_at = int(created_at.timestamp())
    if isinstance(updated_at, datetime):
        updated_at = int(updated_at.timestamp())

    return JobResponse(
        job_id=record.job_id,
        tenant_id=record.tenant_id,
        priority=record.priority,
        state=record.state,
        payload=record.payload,
        retry_count=record.retry_count,
        max_retries=record.max_retries,
        created_at=created_at,
        updated_at=updated_at,
    )


# ---------------------------------------------------------------------------
# Request/response models (Pydantic)
# ---------------------------------------------------------------------------


class EnqueueJobRequest(BaseModel):
    """
    Request body for enqueue.

    The job id comes from the URL path only. This body carries scheduling fields.
    """

    tenant_id: str = Field(..., min_length=1, description="Tenant that owns the job")
    priority: int = Field(..., ge=0, description="Larger value means more urgent")
    payload: Optional[dict[str, Any]] = Field(
        default=None,
        description="Optional client payload stored as JSON",
    )
    max_retries: Optional[int] = Field(
        default=None,
        ge=0,
        description="Maximum retry attempts before dead-lettering (default 3)",
    )


class JobResponse(BaseModel):
    """Public job representation returned by API endpoints."""

    job_id: str
    tenant_id: str
    priority: int
    state: str
    payload: dict[str, Any]
    retry_count: int
    max_retries: int
    created_at: int
    updated_at: int


class MessageResponse(BaseModel):
    """Simple message wrapper used by mutating endpoints."""

    message: str
    job_id: str
    state: str


class JobStateCountsResponse(BaseModel):
    """Postgres job counts grouped by lifecycle state."""

    job_state_counts: dict[str, int]


# ---------------------------------------------------------------------------
# FastAPI application and endpoints
# ---------------------------------------------------------------------------

API_VERSION = "0.1.0"

app = FastAPI(
    title="KernelQ Control Plane API",
    version=API_VERSION,
    description=(
        "Python control-plane API for KernelQ. Jobs are persisted in PostgreSQL. "
        "Scheduling queues and Kafka worker dispatch will be integrated in later steps."
    ),
)


@app.get(
    "/health",
    summary="Shallow health check",
    description=(
        "Returns OK if this process is running. This does not check databases, "
        "Kafka, Redis, or workers—that will be a separate readiness path later."
    ),
)
def health() -> dict[str, str]:
    """Shallow liveness check only."""
    return {
        "status": "ok",
        "service": "kernelq-control-plane",
        "version": API_VERSION,
    }


@app.get(
    "/jobs/{job_id}",
    response_model=JobResponse,
    summary="Get a job by ID",
    description="Return current job state and details from Postgres.",
)
def get_job(job_id: str = Path(..., description="Job ID")) -> JobResponse:
    """Fetch job state/details by id, or return 404 if unknown."""
    repo = get_repository()
    try:
        try:
            record = repo.get_job(job_id)
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while loading job: {exc}",
            ) from exc

        if record is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")

        return _record_to_response(record)
    finally:
        _close_repository(repo)


@app.post(
    "/jobs/{job_id}/enqueue",
    response_model=JobResponse,
    summary="Enqueue a job",
    description=(
        "Validate the request and persist a new job in Postgres with state queued. "
        "The job id is taken from the URL path."
    ),
)
def enqueue_job(
    body: EnqueueJobRequest,
    job_id: str = Path(..., min_length=1, description="Job ID in URL"),
) -> JobResponse:
    """Validate and persist a new job; returns the stored row on success."""
    if not body.tenant_id.strip():
        metrics.record_enqueue_result(EnqueueStatus.REJECTED_INVALID)
        raise HTTPException(status_code=400, detail="tenant_id must not be blank")

    repo = get_repository()
    try:
        try:
            existing = repo.get_job(job_id)
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while checking job: {exc}",
            ) from exc

        if existing is not None:
            raise HTTPException(status_code=409, detail=f"Job {job_id!r} already exists")

        job_payload = body.payload if body.payload is not None else {}
        max_retries = body.max_retries if body.max_retries is not None else 3

        try:
            record = repo.create_job(
                job_id=job_id,
                tenant_id=body.tenant_id,
                priority=body.priority,
                state=JobState.QUEUED.value,
                payload=job_payload,
                max_retries=max_retries,
            )
        except UniqueViolation:
            raise HTTPException(status_code=409, detail=f"Job {job_id!r} already exists")
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while creating job: {exc}",
            ) from exc

        metrics.record_enqueue_result(EnqueueStatus.ACCEPTED)
        return _record_to_response(record)
    finally:
        _close_repository(repo)


@app.post(
    "/jobs/{job_id}/cancel",
    response_model=MessageResponse,
    summary="Cancel a job",
    description="Move a job to canceled if the state machine allows it.",
)
def cancel_job(job_id: str = Path(..., description="Job ID")) -> MessageResponse:
    """Cancel an existing job using can_transition() from job_state."""
    repo = get_repository()
    try:
        try:
            record = repo.get_job(job_id)
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while loading job: {exc}",
            ) from exc

        if record is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")

        current = _parse_state(record.state)
        target = JobState.CANCELED
        if not can_transition(current, target):
            raise HTTPException(
                status_code=409,
                detail=explain_transition(current, target),
            )

        try:
            updated = repo.update_job_state(job_id, target.value)
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while canceling job: {exc}",
            ) from exc

        if updated is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")

        return MessageResponse(
            message="Job canceled",
            job_id=updated.job_id,
            state=updated.state,
        )
    finally:
        _close_repository(repo)


@app.post(
    "/jobs/{job_id}/retry",
    response_model=MessageResponse,
    summary="Retry a failed job",
    description="Move a failed job to retry_scheduled when the state machine allows it.",
)
def retry_job(job_id: str = Path(..., description="Job ID")) -> MessageResponse:
    """Retry a job using can_transition(FAILED, RETRY_SCHEDULED) rules only."""
    repo = get_repository()
    try:
        try:
            record = repo.get_job(job_id)
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while loading job: {exc}",
            ) from exc

        if record is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")

        current = _parse_state(record.state)
        target = JobState.RETRY_SCHEDULED
        if not can_transition(current, target):
            raise HTTPException(
                status_code=409,
                detail=explain_transition(current, target),
            )

        try:
            updated = repo.update_job_state(job_id, target.value)
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while scheduling retry: {exc}",
            ) from exc

        if updated is None:
            raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")

        return MessageResponse(
            message="Job retried",
            job_id=updated.job_id,
            state=updated.state,
        )
    finally:
        _close_repository(repo)


@app.get(
    "/metrics",
    summary="Get current scheduler metrics",
    description="Return current in-process scheduler metrics for this API process.",
)
def get_metrics() -> dict[str, Any]:
    """Expose metrics snapshot for this control-plane process."""
    return metrics.snapshot()


@app.get(
    "/metrics/jobs",
    response_model=JobStateCountsResponse,
    summary="Job counts by lifecycle state",
    description="Return how many jobs exist in each Postgres state.",
)
def get_job_metrics() -> JobStateCountsResponse:
    """Expose durable job state counts from JobRepository."""
    repo = get_repository()
    try:
        try:
            counts = repo.count_jobs_by_state()
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while loading job counts: {exc}",
            ) from exc

        return JobStateCountsResponse(job_state_counts=counts)
    finally:
        _close_repository(repo)


@app.get(
    "/metrics/prometheus",
    summary="Prometheus job state metrics",
    description="Return job counts by Postgres state in Prometheus text exposition format.",
)
def get_prometheus_job_metrics() -> Response:
    """Expose durable job state counts for Prometheus scraping."""
    repo = get_repository()
    try:
        try:
            counts = repo.count_jobs_by_state()
        except PsycopgError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Database error while loading job counts: {exc}",
            ) from exc

        body = format_job_state_counts_for_prometheus(counts)
        return Response(content=body, media_type="text/plain; version=0.0.4")
    finally:
        _close_repository(repo)
