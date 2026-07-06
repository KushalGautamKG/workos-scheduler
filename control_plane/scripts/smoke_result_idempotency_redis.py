#!/usr/bin/env python3
"""
Smoke test: Redis-backed worker-result idempotency (store + key path).

Simulates two result-consumer ``try_claim`` attempts for the same
``(job_id, attempt)`` using ``worker_result_key`` and ``RedisIdempotencyStore``.
Does not run the full Postgres-backed result handler.

Prerequisites:
  - Docker: ``docker compose up -d redis``
  - Container name: ``kernelq-redis``

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/smoke_result_idempotency_redis.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from control_plane.kernelq.idempotency_keys import worker_result_key
from control_plane.kernelq.idempotency_store import RedisIdempotencyStore
from control_plane.kernelq.result_consumer import DEFAULT_DEDUPE_TTL_SECONDS

REDIS_CONTAINER = "kernelq-redis"
DEFAULT_NAMESPACE = "kernelq:idempotency"
ATTEMPT = 0


class DockerRedisCliClient:
    """Duck-typed Redis client backed by ``docker exec … redis-cli``."""

    def set(self, redis_key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool:
        args = ["SET", redis_key, value]
        if nx:
            args.append("NX")
        if ex is not None:
            args.extend(["EX", str(ex)])

        result = subprocess.run(
            ["docker", "exec", REDIS_CONTAINER, "redis-cli", *args],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"redis-cli failed: {result.stderr.strip() or result.stdout.strip()}"
            )
        return result.stdout.strip() == "OK"

    def delete(self, redis_key: str) -> None:
        subprocess.run(
            ["docker", "exec", REDIS_CONTAINER, "redis-cli", "DEL", redis_key],
            capture_output=True,
            text=True,
            check=True,
        )


def _ensure_redis_reachable() -> None:
    result = subprocess.run(
        ["docker", "exec", REDIS_CONTAINER, "redis-cli", "ping"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or result.stdout.strip() != "PONG":
        raise RuntimeError(
            "Redis is not reachable. Start it with: docker compose up -d redis"
        )


def main() -> int:
    _ensure_redis_reachable()

    job_id = f"day101-result-idem-{int(time.time())}"
    logical_key = worker_result_key(job_id, ATTEMPT)
    redis_key = f"{DEFAULT_NAMESPACE}:{logical_key}"

    client = DockerRedisCliClient()
    client.delete(redis_key)

    store = RedisIdempotencyStore(client)
    first_claim = store.try_claim(logical_key, DEFAULT_DEDUPE_TTL_SECONDS)
    second_claim = store.try_claim(logical_key, DEFAULT_DEDUPE_TTL_SECONDS)
    duplicate_skipped = not second_claim

    print(f"job_id={job_id}")
    print(f"attempt={ATTEMPT}")
    print(f"key={logical_key}")
    print(f"first_claim={str(first_claim).lower()}")
    print(f"second_claim={str(second_claim).lower()}")
    print(f"duplicate_skipped={str(duplicate_skipped).lower()}")

    if first_claim and not second_claim and duplicate_skipped:
        print("PASS: redis result idempotency smoke test succeeded")
        print("event=smoke_result_idempotency_redis success=true")
        return 0

    print(
        "FAIL: expected first_claim=true, second_claim=false, duplicate_skipped=true",
        file=sys.stderr,
    )
    print("event=smoke_result_idempotency_redis success=false", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
