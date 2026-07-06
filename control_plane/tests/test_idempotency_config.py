"""Tests for idempotency store configuration from environment."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from control_plane.kernelq.idempotency_config import (
    DEFAULT_REDIS_NAMESPACE,
    RedisCliClient,
    build_idempotency_store_from_env,
    resolve_idempotency_backend,
)
from control_plane.kernelq.idempotency_store import InMemoryIdempotencyStore, RedisIdempotencyStore


def test_default_returns_in_memory_store():
    store = build_idempotency_store_from_env({})
    assert isinstance(store, InMemoryIdempotencyStore)


def test_memory_returns_in_memory_store():
    store = build_idempotency_store_from_env({"KERNELQ_IDEMPOTENCY_BACKEND": "memory"})
    assert isinstance(store, InMemoryIdempotencyStore)


def test_invalid_backend_raises_value_error():
    with pytest.raises(ValueError, match="invalid KERNELQ_IDEMPOTENCY_BACKEND"):
        build_idempotency_store_from_env({"KERNELQ_IDEMPOTENCY_BACKEND": "dynamodb"})


def test_redis_returns_redis_idempotency_store():
    store = build_idempotency_store_from_env({"KERNELQ_IDEMPOTENCY_BACKEND": "redis"})
    assert isinstance(store, RedisIdempotencyStore)


def test_redis_host_port_namespace_env_values_are_used():
    store = build_idempotency_store_from_env(
        {
            "KERNELQ_IDEMPOTENCY_BACKEND": "redis",
            "KERNELQ_REDIS_HOST": "redis.internal",
            "KERNELQ_REDIS_PORT": "6380",
            "KERNELQ_REDIS_NAMESPACE": "kernelq:test",
        }
    )
    assert isinstance(store, RedisIdempotencyStore)
    assert store._namespace == "kernelq:test"  # noqa: SLF001 — test wiring
    assert store._client._host == "redis.internal"  # noqa: SLF001
    assert store._client._port == 6380  # noqa: SLF001


def test_resolve_idempotency_backend_defaults_to_memory():
    assert resolve_idempotency_backend({}) == "memory"
    assert resolve_idempotency_backend({"KERNELQ_IDEMPOTENCY_BACKEND": ""}) == "memory"


def test_redis_cli_client_returns_true_on_ok():
    calls: list[list[str]] = []

    def fake_run(args, **kwargs):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout="OK\n", stderr="")

    client = RedisCliClient("localhost", 6379, run=fake_run)
    assert client.set("kernelq:idempotency:k", "1", nx=True, ex=60) is True
    assert calls[0] == [
        "redis-cli",
        "-h",
        "localhost",
        "-p",
        "6379",
        "SET",
        "kernelq:idempotency:k",
        "1",
        "NX",
        "EX",
        "60",
    ]


def test_redis_cli_client_returns_false_on_non_ok():
    def fake_run(args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    client = RedisCliClient("localhost", 6379, run=fake_run)
    assert client.set("k", "1", nx=True, ex=60) is False


def test_redis_cli_client_returns_false_on_nil_response():
    def fake_run(args, **kwargs):
        return SimpleNamespace(returncode=0, stdout="(nil)\n", stderr="")

    client = RedisCliClient("localhost", 6379, run=fake_run)
    assert client.set("k", "1", nx=True, ex=60) is False


def test_redis_cli_failure_raises_runtime_error():
    def fake_run(args, **kwargs):
        return SimpleNamespace(returncode=1, stdout="", stderr="Connection refused")

    client = RedisCliClient("localhost", 6379, run=fake_run)
    with pytest.raises(RuntimeError, match="redis-cli failed"):
        client.set("k", "1", nx=True, ex=60)


def test_redis_store_uses_default_namespace_when_env_omitted():
    store = build_idempotency_store_from_env({"KERNELQ_IDEMPOTENCY_BACKEND": "redis"})
    assert store._namespace == DEFAULT_REDIS_NAMESPACE  # noqa: SLF001
