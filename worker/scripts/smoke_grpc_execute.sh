#!/usr/bin/env bash
#
# Smoke test: localhost gRPC WorkerExecutionService loopback (Day 117).
# First Execute → SUCCESS; second identical request → DUPLICATE_SKIPPED.
#
# Run from the repository root:
#   ./worker/scripts/smoke_grpc_execute.sh
#
# No Kafka. Uses in-memory execution idempotency inside grpc-server.

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/grpc-server ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

GRPC_ADDR="${KERNELQ_GRPC_ADDR:-127.0.0.1:50051}"
SERVER_BIN="${TMPDIR:-/tmp}/kernelq-grpc-server"
EXECUTE_BIN="${TMPDIR:-/tmp}/kernelq-grpc-execute"
SERVER_LOG="${TMPDIR:-/tmp}/kernelq-grpc-server-smoke.log"
SERVER_PID=""

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_grpc_execute success=false" >&2
  if [[ -f "${SERVER_LOG}" ]]; then
    echo "" >&2
    echo "=== Server logs (${SERVER_LOG}) ===" >&2
    cat "${SERVER_LOG}" >&2
  fi
  exit 1
}

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    echo "Stopping gRPC server (PID ${SERVER_PID})..."
    kill -INT "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
    SERVER_PID=""
  fi
}

trap cleanup EXIT

echo "==> Building gRPC server and execute client..."
(
  cd worker
  go build -o "${SERVER_BIN}" ./cmd/grpc-server
  go build -o "${EXECUTE_BIN}" ./cmd/grpc-execute
)

: >"${SERVER_LOG}"
echo "==> Starting gRPC server on ${GRPC_ADDR}..."
KERNELQ_GRPC_ADDR="${GRPC_ADDR}" \
KERNELQ_WORKER_IDEMPOTENCY_BACKEND=memory \
"${SERVER_BIN}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

# Wait until the listener accepts connections.
READY=0
for _ in $(seq 1 40); do
  if grep -Fq "event=grpc_server_start" "${SERVER_LOG}" 2>/dev/null; then
    READY=1
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    fail "gRPC server exited before becoming ready"
  fi
  sleep 0.1
done
if [[ "${READY}" -ne 1 ]]; then
  fail "gRPC server did not log startup"
fi
sleep 0.2

JOB_ID="test-job"
ATTEMPT=1

echo "==> First Execute (expect SUCCESS)..."
FIRST_OUT="$("${EXECUTE_BIN}" -addr "${GRPC_ADDR}" -job-id "${JOB_ID}" -attempt "${ATTEMPT}" -payload test)"
echo "${FIRST_OUT}"
echo "${FIRST_OUT}" | grep -Fq "status=SUCCESS" || fail "expected status=SUCCESS"
echo "${FIRST_OUT}" | grep -Fq "duplicate_skipped=false" || fail "expected duplicate_skipped=false"

echo "==> Second Execute (expect DUPLICATE_SKIPPED)..."
SECOND_OUT="$("${EXECUTE_BIN}" -addr "${GRPC_ADDR}" -job-id "${JOB_ID}" -attempt "${ATTEMPT}" -payload test)"
echo "${SECOND_OUT}"
echo "${SECOND_OUT}" | grep -Fq "status=DUPLICATE_SKIPPED" || fail "expected status=DUPLICATE_SKIPPED"
echo "${SECOND_OUT}" | grep -Fq "duplicate_skipped=true" || fail "expected duplicate_skipped=true"

echo "PASS: grpc loopback smoke succeeded"
echo "event=smoke_grpc_execute success=true"
