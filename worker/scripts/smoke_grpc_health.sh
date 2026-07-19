#!/usr/bin/env bash
#
# Smoke test: gRPC health lifecycle (Day 118).
# Start server → Check → SERVING → SIGINT → clean exit.
#
# Run from the repository root:
#   ./worker/scripts/smoke_grpc_health.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/grpc-server ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

GRPC_ADDR="${KERNELQ_GRPC_ADDR:-127.0.0.1:50051}"
SERVER_BIN="${TMPDIR:-/tmp}/kernelq-grpc-server-health"
HEALTH_BIN="${TMPDIR:-/tmp}/kernelq-grpc-health"
SERVER_LOG="${TMPDIR:-/tmp}/kernelq-grpc-health-smoke.log"
SERVER_PID=""

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_grpc_health success=false" >&2
  if [[ -f "${SERVER_LOG}" ]]; then
    echo "" >&2
    echo "=== Server logs (${SERVER_LOG}) ===" >&2
    cat "${SERVER_LOG}" >&2
  fi
  exit 1
}

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -INT "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
    SERVER_PID=""
  fi
}

trap cleanup EXIT

echo "==> Building gRPC server and health client..."
(
  cd worker
  go build -o "${SERVER_BIN}" ./cmd/grpc-server
  go build -o "${HEALTH_BIN}" ./cmd/grpc-health
)

: >"${SERVER_LOG}"
echo "==> Starting gRPC server on ${GRPC_ADDR}..."
KERNELQ_GRPC_ADDR="${GRPC_ADDR}" \
KERNELQ_WORKER_IDEMPOTENCY_BACKEND=memory \
"${SERVER_BIN}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

READY=0
for _ in $(seq 1 50); do
  if grep -Fq "event=grpc_server_ready status=SERVING" "${SERVER_LOG}" 2>/dev/null; then
    READY=1
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    fail "gRPC server exited before becoming ready"
  fi
  sleep 0.1
done
if [[ "${READY}" -ne 1 ]]; then
  fail "gRPC server did not become SERVING"
fi

echo "==> Querying health (expect SERVING)..."
HEALTH_OUT="$("${HEALTH_BIN}" -addr "${GRPC_ADDR}")"
echo "${HEALTH_OUT}"
echo "${HEALTH_OUT}" | grep -Fq "status=SERVING" || fail "expected status=SERVING"

echo "==> Stopping gRPC server (SIGINT)..."
kill -INT "${SERVER_PID}" 2>/dev/null || true

# Wait for clean exit (graceful stop).
EXITED=0
for _ in $(seq 1 50); do
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    EXITED=1
    break
  fi
  sleep 0.1
done
if [[ "${EXITED}" -ne 1 ]]; then
  fail "gRPC server did not exit after SIGINT"
fi
wait "${SERVER_PID}" 2>/dev/null || true
SERVER_PID=""

grep -Fq "event=grpc_server_not_ready status=NOT_SERVING" "${SERVER_LOG}" \
  || fail "missing NOT_SERVING transition in server log"
grep -Fq "event=grpc_server_stopped" "${SERVER_LOG}" \
  || fail "missing grpc_server_stopped in server log"

echo "PASS: grpc health smoke succeeded"
echo "event=smoke_grpc_health success=true"
