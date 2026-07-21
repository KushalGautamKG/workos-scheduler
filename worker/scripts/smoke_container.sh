#!/usr/bin/env bash
#
# Smoke test: build and run KernelQ containers (Day 123).
# Builds worker + control-plane images, starts worker, waits for SERVING,
# stops cleanly, and removes containers.
#
# Run from the repository root:
#   ./worker/scripts/smoke_container.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d deploy/docker ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

WORKER_IMAGE="${KERNELQ_WORKER_IMAGE:-kernelq-worker:local}"
CONTROL_IMAGE="${KERNELQ_CONTROL_IMAGE:-kernelq-control-plane:local}"
WORKER_NAME="kernelq-worker-smoke-$$"
CONTROL_NAME="kernelq-control-plane-smoke-$$"
WORKER_LOG="${TMPDIR:-/tmp}/kernelq-container-worker-smoke.log"
CONTROL_LOG="${TMPDIR:-/tmp}/kernelq-container-control-smoke.log"
HOST_PORT="${KERNELQ_SMOKE_GRPC_PORT:-50083}"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_container success=false" >&2
  if [[ -f "${WORKER_LOG}" ]]; then
    echo "" >&2
    echo "=== Worker logs ===" >&2
    cat "${WORKER_LOG}" >&2
  fi
  if [[ -f "${CONTROL_LOG}" ]]; then
    echo "" >&2
    echo "=== Control-plane logs ===" >&2
    cat "${CONTROL_LOG}" >&2
  fi
  exit 1
}

cleanup() {
  docker rm -f "${WORKER_NAME}" >/dev/null 2>&1 || true
  docker rm -f "${CONTROL_NAME}" >/dev/null 2>&1 || true
}

trap cleanup EXIT INT TERM

command -v docker >/dev/null 2>&1 || fail "docker is required"

echo "==> Building worker image (${WORKER_IMAGE})..."
docker build \
  -f deploy/docker/Dockerfile.worker \
  -t "${WORKER_IMAGE}" \
  . || fail "worker image build failed"

echo "==> Building control-plane image (${CONTROL_IMAGE})..."
docker build \
  -f deploy/docker/Dockerfile.control-plane \
  -t "${CONTROL_IMAGE}" \
  . || fail "control-plane image build failed"

: >"${WORKER_LOG}"
: >"${CONTROL_LOG}"

echo "==> Starting worker container..."
docker run -d \
  --name "${WORKER_NAME}" \
  -p "${HOST_PORT}:50051" \
  -e KERNELQ_GRPC_ADDR=0.0.0.0:50051 \
  -e KERNELQ_WORKER_IDEMPOTENCY_BACKEND=memory \
  -e KERNELQ_OTEL_ENABLED=false \
  -e KERNELQ_OTEL_EXPORTER=none \
  "${WORKER_IMAGE}" >/dev/null \
  || fail "failed to start worker container"

READY=0
for _ in $(seq 1 60); do
  docker logs "${WORKER_NAME}" >"${WORKER_LOG}" 2>&1 || true
  if grep -Fq "event=grpc_server_ready status=SERVING" "${WORKER_LOG}" 2>/dev/null; then
    READY=1
    break
  fi
  if ! docker ps --format '{{.Names}}' | grep -Fxq "${WORKER_NAME}"; then
    docker logs "${WORKER_NAME}" >"${WORKER_LOG}" 2>&1 || true
    fail "worker container exited before SERVING"
  fi
  sleep 0.5
done
[[ "${READY}" -eq 1 ]] || fail "worker did not reach SERVING"

echo "==> Starting control-plane container (startup check)..."
docker run -d \
  --name "${CONTROL_NAME}" \
  -p 18000:8000 \
  -e DATABASE_URL="${DATABASE_URL:-postgresql://kernelq:kernelq_dev_password@host.docker.internal:5432/kernelq}" \
  "${CONTROL_IMAGE}" >/dev/null \
  || fail "failed to start control-plane container"

CP_READY=0
for _ in $(seq 1 40); do
  docker logs "${CONTROL_NAME}" >"${CONTROL_LOG}" 2>&1 || true
  if curl -sf "http://127.0.0.1:18000/health" >/dev/null 2>&1; then
    CP_READY=1
    break
  fi
  if ! docker ps --format '{{.Names}}' | grep -Fxq "${CONTROL_NAME}"; then
    # Control plane may exit without Postgres; still require it stayed up long enough
    # to bind, or accept healthy HTTP. Prefer HTTP success.
    docker logs "${CONTROL_NAME}" >"${CONTROL_LOG}" 2>&1 || true
    break
  fi
  sleep 0.5
done
if [[ "${CP_READY}" -ne 1 ]]; then
  # Day 123: image must start; /health is shallow and should work without DB.
  docker logs "${CONTROL_NAME}" >"${CONTROL_LOG}" 2>&1 || true
  if ! grep -Eiq 'uvicorn|started|running' "${CONTROL_LOG}"; then
    fail "control-plane did not become healthy"
  fi
  # Retry health once more after log evidence of start
  curl -sf "http://127.0.0.1:18000/health" >/dev/null 2>&1 || fail "control-plane /health failed"
fi

echo "==> Stopping worker (SIGTERM → graceful NOT_SERVING)..."
docker stop -t 15 "${WORKER_NAME}" >/dev/null || fail "docker stop worker failed"
docker logs "${WORKER_NAME}" >"${WORKER_LOG}" 2>&1 || true
grep -Fq "event=grpc_server_not_ready status=NOT_SERVING" "${WORKER_LOG}" \
  || grep -Fq "event=grpc_server_stopped" "${WORKER_LOG}" \
  || fail "missing graceful shutdown evidence in worker logs"

echo "==> Stopping control-plane..."
docker stop -t 10 "${CONTROL_NAME}" >/dev/null || fail "docker stop control-plane failed"

# Explicit remove so trap does not hide orphans; trap still covers early failure.
docker rm -f "${WORKER_NAME}" >/dev/null 2>&1 || true
docker rm -f "${CONTROL_NAME}" >/dev/null 2>&1 || true

if docker ps -a --format '{{.Names}}' | grep -Eq "^${WORKER_NAME}$|^${CONTROL_NAME}$"; then
  fail "smoke containers still present after cleanup"
fi

echo "PASS: container smoke succeeded"
echo "event=smoke_container success=true"
