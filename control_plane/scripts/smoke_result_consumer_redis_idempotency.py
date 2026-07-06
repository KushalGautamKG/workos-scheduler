#!/usr/bin/env python3
"""
Smoke test: Redis-backed result consumer idempotency (full consumer path).

Uses ``build_idempotency_store_from_env`` (Day 102) + ``ResultConsumerRunner``
(Day 100). Processes the same worker result twice; first handled, second skipped.
No Kafka or Postgres required.

Prerequisites:
  - Docker: ``docker compose up -d redis``
  - Container name: ``kernelq-redis``

Run from the repository root:

    PYTHONPATH=. python3 control_plane/scripts/smoke_result_consumer_redis_idempotency.py
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from control_plane.kernelq.idempotency_config import (
    DEFAULT_REDIS_HOST,
    DEFAULT_REDIS_NAMESPACE,
    DEFAULT_REDIS_PORT,
    build_idempotency_store_from_env,
)
from control_plane.kernelq.idempotency_keys import worker_result_key
from control_plane.kernelq.result_consumer import ResultConsumerRunner, ResultHandler, ResultMessage
from control_plane.kernelq.result_event import WORKER_RESULT_EVENT_TYPE, WorkerResultEvent

REDIS_CONTAINER = "kernelq-redis"
ATTEMPT = 0

SMOKE_ENV = {
    "KERNELQ_IDEMPOTENCY_BACKEND": "redis",
    "KERNELQ_REDIS_HOST": DEFAULT_REDIS_HOST,
    "KERNELQ_REDIS_PORT": str(DEFAULT_REDIS_PORT),
    "KERNELQ_REDIS_NAMESPACE": DEFAULT_REDIS_NAMESPACE,
}


class RecordingResultHandler(ResultHandler):
    """Records how many validated events reached the handler."""

    def __init__(self) -> None:
        self.events: list[WorkerResultEvent] = []

    def handle(self, event: WorkerResultEvent) -> None:
        self.events.append(event)

    @property
    def handled_count(self) -> int:
        return len(self.events)


def _ensure_redis_reachable() -> None:
    result = subprocess.run(
        ["docker", "exec", REDIS_CONTAINER, "redis-cli", "ping"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or result.stdout.strip() != "PONG":
        raise RuntimeError(
            f"Redis is not reachable via {REDIS_CONTAINER}. "
            "Start it with: docker compose up -d redis"
        )


def _docker_exec_redis_cli_run(args, **kwargs):
    """Route ``redis-cli`` calls through ``docker exec kernelq-redis`` (no host binary)."""
    # args: ["redis-cli", "-h", host, "-p", port, <redis-command>...]
    redis_command = args[5:]
    return subprocess.run(
        ["docker", "exec", REDIS_CONTAINER, "redis-cli", *redis_command],
        **kwargs,
    )


def _build_redis_store_for_smoke() -> object:
    """Day 102 env path with docker-exec redis-cli for local smoke (no host ``redis-cli``)."""
    store = build_idempotency_store_from_env(SMOKE_ENV)
    # Smoke hosts often lack redis-cli; container has it (same pattern as Day 101 smokes).
    store._client._run = _docker_exec_redis_cli_run  # noqa: SLF001
    return store


def _clear_redis_key(redis_key: str) -> None:
    subprocess.run(
        ["docker", "exec", REDIS_CONTAINER, "redis-cli", "DEL", redis_key],
        capture_output=True,
        text=True,
        check=True,
    )


def _result_message(job_id: str, attempt: int) -> ResultMessage:
    payload = {
        "event_type": WORKER_RESULT_EVENT_TYPE,
        "job_id": job_id,
        "status": "succeeded",
        "message": "smoke",
        "worker": "smoke-result-consumer",
        "attempt": attempt,
    }
    return ResultMessage(key=job_id, value=json.dumps(payload).encode())


def main() -> int:
    _ensure_redis_reachable()

    job_id = f"day103-result-consumer-redis-{int(time.time())}"
    logical_key = worker_result_key(job_id, ATTEMPT)
    redis_key = f"{DEFAULT_REDIS_NAMESPACE}:{logical_key}"
    _clear_redis_key(redis_key)

    handler = RecordingResultHandler()
    store = _build_redis_store_for_smoke()
    consumer = ResultConsumerRunner(handler, idempotency_store=store)

    message = _result_message(job_id, ATTEMPT)
    consumer.process_message(message)
    consumer.process_message(message)

    handled_count = handler.handled_count
    duplicate_messages = consumer.duplicate_messages
    first_processed = handled_count == 1
    second_skipped = duplicate_messages == 1 and handled_count == 1

    print(f"job_id={job_id}")
    print(f"attempt={ATTEMPT}")
    print(f"key={logical_key}")
    print(f"handled_count={handled_count}")
    print(f"duplicate_messages={duplicate_messages}")
    print(f"first_processed={str(first_processed).lower()}")
    print(f"second_skipped={str(second_skipped).lower()}")

    if first_processed and second_skipped:
        print("PASS: redis result consumer idempotency smoke test succeeded")
        print("event=smoke_result_consumer_redis_idempotency success=true")
        return 0

    print(
        "FAIL: expected handled_count=1, duplicate_messages=1, "
        "first_processed=true, second_skipped=true",
        file=sys.stderr,
    )
    print("event=smoke_result_consumer_redis_idempotency success=false", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
