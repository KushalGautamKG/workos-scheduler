"""
Tests for canonical idempotency key builders.
"""

import pytest

from control_plane.kernelq.idempotency_keys import (
    dispatch_key,
    event_key,
    execution_key,
    worker_result_key,
)


def test_worker_result_key_format():
    assert worker_result_key("job-abc", 0) == "worker-result:job-abc:0"
    assert worker_result_key("job-abc", 2) == "worker-result:job-abc:2"


def test_dispatch_key_format():
    assert dispatch_key("job-abc", 0) == "dispatch:job-abc:0"
    assert dispatch_key("job-abc", 1) == "dispatch:job-abc:1"


def test_execution_key_format():
    assert execution_key("job-abc", 0) == "execution:job-abc:0"
    assert execution_key("job-abc", 3) == "execution:job-abc:3"


def test_event_key_format():
    assert event_key("evt-7f3a") == "event:evt-7f3a"


@pytest.mark.parametrize(
    "builder",
    [worker_result_key, dispatch_key, execution_key],
)
def test_empty_job_id_raises_value_error(builder):
    with pytest.raises(ValueError, match="job_id must be a non-empty string"):
        builder("", 0)
    with pytest.raises(ValueError, match="job_id must be a non-empty string"):
        builder("   ", 0)


def test_empty_event_id_raises_value_error():
    with pytest.raises(ValueError, match="event_id must be a non-empty string"):
        event_key("")
    with pytest.raises(ValueError, match="event_id must be a non-empty string"):
        event_key("   ")


@pytest.mark.parametrize(
    "builder",
    [worker_result_key, dispatch_key, execution_key],
)
def test_negative_attempt_raises_value_error(builder):
    with pytest.raises(ValueError, match="attempt must be a non-negative integer"):
        builder("job-abc", -1)


@pytest.mark.parametrize(
    "builder",
    [worker_result_key, dispatch_key, execution_key],
)
def test_non_int_attempt_raises_value_error(builder):
    with pytest.raises(ValueError, match="attempt must be a non-negative integer"):
        builder("job-abc", "0")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="attempt must be a non-negative integer"):
        builder("job-abc", 1.5)  # type: ignore[arg-type]


def test_different_prefixes_produce_distinct_keys():
    job_id = "job-abc"
    attempt = 0
    keys = {
        worker_result_key(job_id, attempt),
        dispatch_key(job_id, attempt),
        execution_key(job_id, attempt),
    }
    assert len(keys) == 3


def test_attempt_is_included_in_key():
    job_id = "job-abc"
    assert worker_result_key(job_id, 0) != worker_result_key(job_id, 1)
    assert dispatch_key(job_id, 0) != dispatch_key(job_id, 1)
    assert execution_key(job_id, 0) != execution_key(job_id, 1)
