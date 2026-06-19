"""
Beginner-friendly API tests for the FastAPI control-plane app.

These tests call the real Postgres-backed API. Before running:

1. Start Postgres: ``docker compose up -d postgres``
2. Apply the migration once:

       psql "$DATABASE_URL" -f control_plane/migrations/001_create_jobs.sql

Each test uses a unique ``job_id``. Rows are deleted before and after so runs stay isolated.
Enqueue request bodies never include ``job_id`` — the id comes from the URL path only.
"""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient
from psycopg import OperationalError

import control_plane.api as api_module
from control_plane.api import app
from control_plane.kernelq.db import connect
from control_plane.kernelq.job_repository import JobRepository
from control_plane.kernelq.scheduler_metrics import SchedulerMetrics


TEST_PREFIX = "test-api-"


def _unique_job_id(prefix: str) -> str:
    """Return a unique job id so parallel or repeated runs do not collide."""
    # Keep a consistent prefix so cleanup can safely target rows from this file.
    return f"{TEST_PREFIX}{prefix}_{uuid.uuid4().hex[:16]}"


def _enqueue_body(*, tenant_id: str = "tenant-a", priority: int = 5) -> dict:
    """Valid enqueue JSON body (job_id is only in the URL)."""
    return {"tenant_id": tenant_id, "priority": priority}


def _delete_job(job_id: str) -> None:
    """Remove a test job row via JobRepository (safe if the job does not exist)."""
    with connect() as conn:
        JobRepository(conn).delete_job(job_id)


def _cleanup_jobs_with_test_prefix() -> None:
    """Delete all jobs created by this test module."""
    with connect() as conn:
        conn.execute(
            "DELETE FROM jobs WHERE job_id LIKE %(job_id_prefix)s",
            {"job_id_prefix": f"{TEST_PREFIX}%"},
        )
        conn.commit()


@pytest.fixture(scope="module", autouse=True)
def _require_postgres_and_migration() -> None:
    """Skip the module unless Postgres is up and the jobs table exists."""
    try:
        with connect() as conn:
            conn.execute("SELECT 1")
            row = conn.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'jobs'
                """
            ).fetchone()
    except OperationalError as exc:
        pytest.skip(f"Postgres not reachable (start docker compose): {exc}")

    if row is None:
        pytest.skip(
            "jobs table missing — apply control_plane/migrations/001_create_jobs.sql"
        )


@pytest.fixture(autouse=True)
def reset_metrics() -> None:
    """Reset in-process metrics so test order does not matter."""
    api_module.metrics = SchedulerMetrics()


@pytest.fixture(autouse=True)
def cleanup_test_jobs() -> None:
    """
    Run cleanup before and after each test so the module never depends on an
    empty database or on side-effects from previous tests.
    """
    _cleanup_jobs_with_test_prefix()
    yield
    _cleanup_jobs_with_test_prefix()


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


# 1) GET /metrics returns 200
def test_get_metrics_returns_200(client: TestClient) -> None:
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "enqueue_accepted_count" in response.json()


# 1b) GET /metrics/jobs returns job state counts from Postgres
def test_get_job_metrics_returns_200_and_shape(client: TestClient) -> None:
    response = client.get("/metrics/jobs")

    assert response.status_code == 200
    body = response.json()
    assert "job_state_counts" in body

    counts = body["job_state_counts"]
    assert isinstance(counts, dict)
    for state, count in counts.items():
        assert isinstance(state, str)
        assert isinstance(count, int)
        assert count >= 0


# 1c) GET /metrics/prometheus returns Prometheus text exposition
def test_get_prometheus_metrics_returns_text_exposition(client: TestClient) -> None:
    response = client.get("/metrics/prometheus")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")

    text = response.text
    assert "# HELP kernelq_jobs_by_state" in text
    assert "# TYPE kernelq_jobs_by_state gauge" in text
    assert "kernelq_jobs_by_state" in text


# 1d) GET /metrics/durations returns job duration averages from Postgres
def test_get_job_duration_metrics_returns_200_and_shape(client: TestClient) -> None:
    response = client.get("/metrics/durations")

    assert response.status_code == 200
    body = response.json()

    assert "completed_jobs_count" in body
    assert "average_queue_wait_seconds" in body
    assert "average_completion_seconds" in body

    assert isinstance(body["completed_jobs_count"], int)
    assert isinstance(body["average_queue_wait_seconds"], (int, float))
    assert isinstance(body["average_completion_seconds"], (int, float))


# 2) GET missing job returns 404
def test_get_missing_job_returns_404(client: TestClient) -> None:
    job_id = _unique_job_id("missing")
    response = client.get(f"/jobs/{job_id}")

    assert response.status_code == 404


# 3) POST enqueue with valid body returns 200
def test_enqueue_valid_body_returns_200(client: TestClient) -> None:
    job_id = _unique_job_id("enqueue_ok")
    _delete_job(job_id)

    try:
        response = client.post(
            f"/jobs/{job_id}/enqueue",
            json=_enqueue_body(),
        )

        assert response.status_code == 200
        body = response.json()
        assert body["job_id"] == job_id
        assert body["state"] == "queued"
        assert body["tenant_id"] == "tenant-a"
        assert body["priority"] == 5
    finally:
        _delete_job(job_id)


# 4) GET after enqueue returns persisted job data
def test_get_job_after_enqueue_returns_persisted_job_data(client: TestClient) -> None:
    job_id = _unique_job_id("get_after_enqueue")
    _delete_job(job_id)

    try:
        client.post(f"/jobs/{job_id}/enqueue", json=_enqueue_body())

        response = client.get(f"/jobs/{job_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["job_id"] == job_id
        assert data["tenant_id"] == "tenant-a"
        assert data["priority"] == 5
        assert data["state"] == "queued"
        assert data["payload"] == {}
        assert data["retry_count"] == 0
        assert data["max_retries"] == 3
        assert data["created_at"] is not None
        assert data["updated_at"] is not None
    finally:
        _delete_job(job_id)


# 5) duplicate enqueue returns 409
def test_enqueue_duplicate_job_returns_409(client: TestClient) -> None:
    job_id = _unique_job_id("enqueue_dup")
    body = _enqueue_body(priority=1)
    _delete_job(job_id)

    try:
        first = client.post(f"/jobs/{job_id}/enqueue", json=body)
        assert first.status_code == 200

        second = client.post(f"/jobs/{job_id}/enqueue", json=body)
        assert second.status_code == 409
    finally:
        _delete_job(job_id)


# 6) POST cancel on QUEUED job returns 200 and state canceled
def test_cancel_queued_job_returns_200_and_canceled_state(client: TestClient) -> None:
    job_id = _unique_job_id("cancel_queued")
    _delete_job(job_id)

    try:
        enqueue = client.post(f"/jobs/{job_id}/enqueue", json=_enqueue_body())
        assert enqueue.status_code == 200
        assert enqueue.json()["state"] == "queued"

        response = client.post(f"/jobs/{job_id}/cancel")

        assert response.status_code == 200
        assert response.json()["state"] == "canceled"
    finally:
        _delete_job(job_id)


# 7) POST retry after cancel returns 409
def test_retry_after_cancel_returns_409(client: TestClient) -> None:
    job_id = _unique_job_id("retry_after_cancel")
    _delete_job(job_id)

    try:
        client.post(f"/jobs/{job_id}/enqueue", json=_enqueue_body())
        client.post(f"/jobs/{job_id}/cancel")

        response = client.post(f"/jobs/{job_id}/retry")

        assert response.status_code == 409
    finally:
        _delete_job(job_id)


# 8) POST enqueue missing required fields returns 422
def test_enqueue_missing_required_fields_returns_422(client: TestClient) -> None:
    job_id = _unique_job_id("missing_required")
    response = client.post(
        f"/jobs/{job_id}/enqueue",
        json={"priority": 5},
    )

    assert response.status_code == 422


# 9) POST enqueue blank tenant_id returns non-200
def test_enqueue_blank_tenant_id_returns_non_200(client: TestClient) -> None:
    job_id = _unique_job_id("blank_tenant")
    _delete_job(job_id)

    response = client.post(
        f"/jobs/{job_id}/enqueue",
        json=_enqueue_body(tenant_id="   "),
    )

    assert response.status_code != 200
    _delete_job(job_id)


# 10) POST enqueue negative priority returns 422 or 400
def test_enqueue_negative_priority_returns_422_or_400(client: TestClient) -> None:
    job_id = _unique_job_id("bad_priority")
    _delete_job(job_id)

    response = client.post(
        f"/jobs/{job_id}/enqueue",
        json=_enqueue_body(priority=-1),
    )

    assert response.status_code in (400, 422)
    _delete_job(job_id)


# 11) retry works only from FAILED -> RETRY_SCHEDULED
def test_retry_from_failed_returns_200_and_retry_scheduled_state(
    client: TestClient,
) -> None:
    job_id = _unique_job_id("retry_failed")
    _delete_job(job_id)

    try:
        with connect() as conn:
            repo = JobRepository(conn)
            repo.create_job(
                job_id,
                tenant_id="tenant-a",
                priority=5,
                state="failed",
                payload={},
            )

        response = client.post(f"/jobs/{job_id}/retry")

        assert response.status_code == 200
        assert response.json()["state"] == "retry_scheduled"

        loaded = client.get(f"/jobs/{job_id}")
        assert loaded.status_code == 200
        assert loaded.json()["state"] == "retry_scheduled"
    finally:
        _delete_job(job_id)
