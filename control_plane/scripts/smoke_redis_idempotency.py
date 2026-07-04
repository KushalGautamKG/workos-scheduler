#!/usr/bin/env python3
"""
Optional smoke test: Redis SET NX EX idempotency via docker exec redis-cli.

Uses a duck-typed client with ``RedisIdempotencyStore`` — no ``redis`` Python
package required.

Prerequisites:
  - Docker: ``docker compose up -d redis``
  - Container name: ``kernelq-redis``

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/smoke_redis_idempotency.py
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from control_plane.kernelq.idempotency_store import RedisIdempotencyStore

REDIS_CONTAINER = "kernelq-redis"
SMOKE_NAMESPACE = "kernelq:smoke:idempotency"
TTL_SECONDS = 60


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


def main() -> int:
    claim_key = f"run-{int(time.time())}-{uuid.uuid4().hex[:8]}"
    client = DockerRedisCliClient()
    store = RedisIdempotencyStore(client, namespace=SMOKE_NAMESPACE)

    redis_key = f"{SMOKE_NAMESPACE}:{claim_key}"
    client.delete(redis_key)

    first_claim = store.try_claim(claim_key, TTL_SECONDS)
    second_claim = store.try_claim(claim_key, TTL_SECONDS)

    print(f"first_claim={str(first_claim).lower()}")
    print(f"second_claim={str(second_claim).lower()}")

    if first_claim and not second_claim:
        print("PASS: redis idempotency smoke test succeeded")
        print("event=smoke_redis_idempotency success=true")
        return 0

    print("FAIL: expected first_claim=true and second_claim=false", file=sys.stderr)
    print("event=smoke_redis_idempotency success=false", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
