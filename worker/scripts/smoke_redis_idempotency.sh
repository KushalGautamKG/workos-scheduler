#!/usr/bin/env bash
#
# Smoke test: Go RedisIdempotencyStore against live Redis (SET NX EX).
#
# Run from the repository root:
#   ./worker/scripts/smoke_redis_idempotency.sh
#
# Requires Docker Redis (kernelq-redis). No Kafka/Postgres.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${REPO_ROOT}"

if ! docker compose ps --status running --services 2>/dev/null | grep -qx redis; then
  echo "starting redis..."
  docker compose up -d redis
fi

# Wait briefly for Redis to accept connections.
for _ in $(seq 1 30); do
  if docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.2
done

if ! docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
  echo "FAIL: redis not ready (kernelq-redis)" >&2
  echo "event=smoke_go_redis_idempotency success=false" >&2
  exit 1
fi

cd "${WORKER_DIR}"
go run ./cmd/smoke_redis_idempotency
