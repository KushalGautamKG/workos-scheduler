"""
Build an ``IdempotencyStore`` from environment variables.

Stdlib only — Redis backend uses ``redis-cli`` via subprocess (no ``redis`` package).

Env:
  ``KERNELQ_IDEMPOTENCY_BACKEND`` — ``memory`` (default) or ``redis``
  ``KERNELQ_REDIS_HOST`` — default ``localhost``
  ``KERNELQ_REDIS_PORT`` — default ``6379``
  ``KERNELQ_REDIS_NAMESPACE`` — default ``kernelq:idempotency``
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Callable, Mapping
from typing import Any

from .idempotency_store import IdempotencyStore, InMemoryIdempotencyStore, RedisIdempotencyStore

ENV_BACKEND = "KERNELQ_IDEMPOTENCY_BACKEND"
ENV_REDIS_HOST = "KERNELQ_REDIS_HOST"
ENV_REDIS_PORT = "KERNELQ_REDIS_PORT"
ENV_REDIS_NAMESPACE = "KERNELQ_REDIS_NAMESPACE"

DEFAULT_BACKEND = "memory"
DEFAULT_REDIS_HOST = "localhost"
DEFAULT_REDIS_PORT = 6379
DEFAULT_REDIS_NAMESPACE = "kernelq:idempotency"


class RedisCliClient:
    """
    Duck-typed Redis client: ``redis-cli -h <host> -p <port> SET … NX EX …``.

    Returns ``True`` when stdout is ``OK``, ``False`` otherwise (e.g. duplicate ``NX``).
    """

    def __init__(
        self,
        host: str,
        port: int,
        *,
        run: Callable[..., Any] | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._run = run if run is not None else subprocess.run

    def set(self, redis_key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool:
        args = ["redis-cli", "-h", self._host, "-p", str(self._port), "SET", redis_key, value]
        if nx:
            args.append("NX")
        if ex is not None:
            args.extend(["EX", str(ex)])

        result = self._run(
            args,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"redis-cli failed: {result.stderr.strip() or result.stdout.strip()}"
            )
        return result.stdout.strip() == "OK"


def _env_mapping(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def resolve_idempotency_backend(env: Mapping[str, str] | None = None) -> str:
    """Return normalized backend name: ``memory`` or ``redis``."""
    raw = _env_mapping(env).get(ENV_BACKEND, "").strip().lower()
    if not raw or raw == "memory":
        return "memory"
    if raw == "redis":
        return "redis"
    raise ValueError(f"invalid {ENV_BACKEND}: {raw!r} (expected 'memory' or 'redis')")


def build_idempotency_store_from_env(env: Mapping[str, str] | None = None) -> IdempotencyStore:
    """
    Construct the idempotency store for the current environment.

    Missing or ``memory`` → ``InMemoryIdempotencyStore``.
    ``redis`` → ``RedisIdempotencyStore`` with ``RedisCliClient``.
    """
    mapping = _env_mapping(env)
    backend = resolve_idempotency_backend(mapping)

    if backend == "memory":
        return InMemoryIdempotencyStore()

    host = mapping.get(ENV_REDIS_HOST, DEFAULT_REDIS_HOST).strip() or DEFAULT_REDIS_HOST
    port_raw = mapping.get(ENV_REDIS_PORT, str(DEFAULT_REDIS_PORT)).strip()
    try:
        port = int(port_raw)
    except ValueError as exc:
        raise ValueError(f"invalid {ENV_REDIS_PORT}: {port_raw!r}") from exc

    namespace = mapping.get(ENV_REDIS_NAMESPACE, DEFAULT_REDIS_NAMESPACE).strip()
    if not namespace:
        namespace = DEFAULT_REDIS_NAMESPACE

    client = RedisCliClient(host=host, port=port)
    return RedisIdempotencyStore(client, namespace=namespace)
