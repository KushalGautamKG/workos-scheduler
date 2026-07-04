"""
Tests for the idempotency store boundary and in-memory implementation.
"""

import pytest

from control_plane.kernelq.idempotency_store import InMemoryIdempotencyStore


def _store_at(fixed_time: float) -> InMemoryIdempotencyStore:
    """Build a store whose clock always reads ``fixed_time``."""
    return InMemoryIdempotencyStore(now=lambda: fixed_time)


def test_first_claim_returns_true():
    store = _store_at(1000.0)
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is True


def test_duplicate_before_ttl_returns_false():
    store = _store_at(1000.0)
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is True
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is False


def test_claim_after_ttl_expiry_returns_true():
    current_time = 1000.0

    def now() -> float:
        return current_time

    store = InMemoryIdempotencyStore(now=now)
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is True
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is False

    current_time = 1061.0
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is True


def test_empty_key_raises_value_error():
    store = _store_at(1000.0)
    with pytest.raises(ValueError, match="key must be non-empty"):
        store.try_claim("", ttl_seconds=60)
    with pytest.raises(ValueError, match="key must be non-empty"):
        store.try_claim("   ", ttl_seconds=60)


@pytest.mark.parametrize("ttl_seconds", [0, -1])
def test_non_positive_ttl_raises_value_error(ttl_seconds: int):
    store = _store_at(1000.0)
    with pytest.raises(ValueError, match="ttl_seconds must be a positive integer"):
        store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=ttl_seconds)


def test_cleanup_expired_removes_expired_keys():
    current_time = 1000.0

    def now() -> float:
        return current_time

    store = InMemoryIdempotencyStore(now=now)
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is True
    assert store.try_claim("kernelq:dispatch:job-2:0", ttl_seconds=120) is True

    current_time = 1070.0
    assert store.cleanup_expired() == 1
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is True
    assert store.try_claim("kernelq:dispatch:job-2:0", ttl_seconds=120) is False


def test_separate_keys_do_not_collide():
    store = _store_at(1000.0)
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is True
    assert store.try_claim("kernelq:dispatch:job-2:0", ttl_seconds=60) is True
    assert store.try_claim("kernelq:dispatch:job-1:0", ttl_seconds=60) is False
    assert store.try_claim("kernelq:dispatch:job-2:0", ttl_seconds=60) is False


def test_injectable_clock_makes_tests_deterministic():
    times = iter([1000.0, 1000.0, 1010.0, 1010.0])

    store = InMemoryIdempotencyStore(now=lambda: next(times))
    assert store.try_claim("kernelq:execution:job-a:0", ttl_seconds=10) is True
    assert store.try_claim("kernelq:execution:job-a:0", ttl_seconds=10) is False
    assert store.try_claim("kernelq:execution:job-a:0", ttl_seconds=10) is True
    assert store.try_claim("kernelq:execution:job-a:0", ttl_seconds=10) is False
