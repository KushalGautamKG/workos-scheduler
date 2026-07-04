"""
Tests for RedisIdempotencyStore with a fake duck-typed Redis client.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from control_plane.kernelq.idempotency_store import RedisIdempotencyStore


@dataclass
class FakeRedisSetCall:
    redis_key: str
    value: str
    nx: bool
    ex: int


@dataclass
class FakeRedisClient:
    """Minimal Redis client stub — no broker required."""

    set_results: list[bool | None] = field(default_factory=list)
    calls: list[FakeRedisSetCall] = field(default_factory=list)
    error: Exception | None = None

    def set(self, redis_key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
        if self.error is not None:
            raise self.error
        self.calls.append(
            FakeRedisSetCall(
                redis_key=redis_key,
                value=value,
                nx=nx,
                ex=ex if ex is not None else 0,
            )
        )
        if not self.set_results:
            return True
        return self.set_results.pop(0)


def test_first_claim_calls_set_with_nx_and_ex_and_returns_true():
    client = FakeRedisClient(set_results=[True])
    store = RedisIdempotencyStore(client)

    assert store.try_claim("worker-result:job-1:0", ttl_seconds=60) is True
    assert len(client.calls) == 1
    assert client.calls[0].redis_key == "kernelq:idempotency:worker-result:job-1:0"
    assert client.calls[0].value == "1"
    assert client.calls[0].nx is True
    assert client.calls[0].ex == 60


def test_duplicate_claim_returns_false():
    client = FakeRedisClient(set_results=[True, None])
    store = RedisIdempotencyStore(client)

    assert store.try_claim("worker-result:job-1:0", ttl_seconds=60) is True
    assert store.try_claim("worker-result:job-1:0", ttl_seconds=60) is False


def test_key_is_namespaced():
    client = FakeRedisClient(set_results=[True])
    store = RedisIdempotencyStore(client, namespace="kernelq:dedupe")

    store.try_claim("evt-abc", ttl_seconds=30)

    assert client.calls[0].redis_key == "kernelq:dedupe:evt-abc"


def test_empty_namespace_raises_value_error():
    with pytest.raises(ValueError, match="namespace must be non-empty"):
        RedisIdempotencyStore(FakeRedisClient(), namespace="")


def test_empty_key_raises_value_error():
    store = RedisIdempotencyStore(FakeRedisClient())
    with pytest.raises(ValueError, match="key must be non-empty"):
        store.try_claim("", ttl_seconds=60)


@pytest.mark.parametrize("ttl_seconds", [0, -1])
def test_non_positive_ttl_raises_value_error(ttl_seconds: int):
    store = RedisIdempotencyStore(FakeRedisClient())
    with pytest.raises(ValueError, match="ttl_seconds must be a positive integer"):
        store.try_claim("worker-result:job-1:0", ttl_seconds=ttl_seconds)


def test_client_error_propagates():
    client = FakeRedisClient(error=RuntimeError("redis unavailable"))
    store = RedisIdempotencyStore(client)

    with pytest.raises(RuntimeError, match="redis unavailable"):
        store.try_claim("worker-result:job-1:0", ttl_seconds=60)


def test_redis_client_receives_value_one():
    client = FakeRedisClient(set_results=[True])
    store = RedisIdempotencyStore(client)

    store.try_claim("worker-result:job-1:0", ttl_seconds=60)

    assert client.calls[0].value == "1"
