#!/usr/bin/env bash
#
# Smoke test: worker.execute span on stdout exporter (Day 120).
# One gRPC Execute → span with job.id, job.attempt, execution.status.
#
# Run from the repository root:
#   ./worker/scripts/smoke_worker_trace.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/grpc-server ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

GRPC_ADDR="${KERNELQ_GRPC_ADDR:-127.0.0.1:50081}"
SERVER_BIN="${TMPDIR:-/tmp}/kernelq-grpc-server-trace"
EXECUTE_BIN="${TMPDIR:-/tmp}/kernelq-grpc-execute-trace"
SERVER_LOG="${TMPDIR:-/tmp}/kernelq-worker-trace-smoke.log"
SERVER_PID=""

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_worker_trace success=false" >&2
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

echo "==> Building gRPC server and execute client..."
(
  cd worker
  go build -o "${SERVER_BIN}" ./cmd/grpc-server
  go build -o "${EXECUTE_BIN}" ./cmd/grpc-execute
)

: >"${SERVER_LOG}"
echo "==> Starting gRPC server with stdout tracer on ${GRPC_ADDR}..."
KERNELQ_GRPC_ADDR="${GRPC_ADDR}" \
KERNELQ_WORKER_IDEMPOTENCY_BACKEND=memory \
KERNELQ_OTEL_ENABLED=true \
KERNELQ_OTEL_EXPORTER=stdout \
KERNELQ_OTEL_SERVICE_NAME=kernelq-worker-trace-smoke \
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
[[ "${READY}" -eq 1 ]] || fail "gRPC server did not become SERVING"

JOB_ID="day120-trace-job"
ATTEMPT=0

echo "==> Executing job (expect SUCCESS + worker.execute span)..."
OUT="$("${EXECUTE_BIN}" -addr "${GRPC_ADDR}" -job-id "${JOB_ID}" -attempt "${ATTEMPT}" -payload smoke)"
echo "${OUT}"
echo "${OUT}" | grep -Fq "status=SUCCESS" || fail "expected status=SUCCESS"

echo "==> Stopping server to flush spans..."
kill -INT "${SERVER_PID}" 2>/dev/null || true
for _ in $(seq 1 50); do
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
wait "${SERVER_PID}" 2>/dev/null || true
SERVER_PID=""

grep -Fq "worker.execute" "${SERVER_LOG}" || fail "missing worker.execute in exporter output"
grep -Eq 'job\.id|"job.id"' "${SERVER_LOG}" || fail "missing job.id attribute"
grep -Eq 'job\.attempt|"job.attempt"' "${SERVER_LOG}" || fail "missing job.attempt attribute"
grep -Eq 'execution\.status|"execution.status"' "${SERVER_LOG}" || fail "missing execution.status attribute"

echo "PASS: worker tracing smoke succeeded"
echo "event=smoke_worker_trace success=true"
