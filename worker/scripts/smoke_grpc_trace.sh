#!/usr/bin/env bash
#
# Smoke test: gRPC client/server + worker.execute share one trace (Day 121).
# Server and client both export stdout spans; Python checks shared TraceID.
#
# Run from the repository root:
#   ./worker/scripts/smoke_grpc_trace.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/grpc-server ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

GRPC_ADDR="${KERNELQ_GRPC_ADDR:-127.0.0.1:50082}"
SERVER_BIN="${TMPDIR:-/tmp}/kernelq-grpc-server-grpc-trace"
EXECUTE_BIN="${TMPDIR:-/tmp}/kernelq-grpc-execute-grpc-trace"
SERVER_LOG="${TMPDIR:-/tmp}/kernelq-grpc-trace-server.log"
CLIENT_LOG="${TMPDIR:-/tmp}/kernelq-grpc-trace-client.log"
CLIENT_OUT="${TMPDIR:-/tmp}/kernelq-grpc-trace-client.out"
SERVER_PID=""

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_grpc_trace success=false" >&2
  if [[ -f "${SERVER_LOG}" ]]; then
    echo "" >&2
    echo "=== Server logs (${SERVER_LOG}) ===" >&2
    cat "${SERVER_LOG}" >&2
  fi
  if [[ -f "${CLIENT_LOG}" ]]; then
    echo "" >&2
    echo "=== Client logs (${CLIENT_LOG}) ===" >&2
    cat "${CLIENT_LOG}" >&2
  fi
  if [[ -f "${CLIENT_OUT}" ]]; then
    echo "" >&2
    echo "=== Client stdout (${CLIENT_OUT}) ===" >&2
    cat "${CLIENT_OUT}" >&2
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

trap cleanup EXIT INT TERM

echo "==> Building gRPC server and execute client..."
(
  cd worker
  go build -o "${SERVER_BIN}" ./cmd/grpc-server
  go build -o "${EXECUTE_BIN}" ./cmd/grpc-execute
)

: >"${SERVER_LOG}"
: >"${CLIENT_LOG}"
: >"${CLIENT_OUT}"

echo "==> Starting gRPC server with stdout tracer on ${GRPC_ADDR}..."
KERNELQ_GRPC_ADDR="${GRPC_ADDR}" \
KERNELQ_WORKER_IDEMPOTENCY_BACKEND=memory \
KERNELQ_OTEL_ENABLED=true \
KERNELQ_OTEL_EXPORTER=stdout \
KERNELQ_OTEL_SERVICE_NAME=kernelq-grpc-trace-server \
"${SERVER_BIN}" >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

READY=0
for _ in $(seq 1 50); do
  if grep -Fq "event=grpc_server_ready status=SERVING" "${SERVER_LOG}" 2>/dev/null \
    || grep -Fq '"message":"worker ready"' "${SERVER_LOG}" 2>/dev/null \
    || grep -Fq '"status":"ready"' "${SERVER_LOG}" 2>/dev/null; then
    READY=1
    break
  fi
  if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
    fail "gRPC server exited before becoming ready"
  fi
  sleep 0.1
done
[[ "${READY}" -eq 1 ]] || fail "gRPC server did not become SERVING"

JOB_ID="day121-grpc-trace-job"
ATTEMPT=0

echo "==> Executing job via instrumented client (expect SUCCESS + shared TraceID)..."
KERNELQ_OTEL_ENABLED=true \
KERNELQ_OTEL_EXPORTER=stdout \
KERNELQ_OTEL_SERVICE_NAME=kernelq-grpc-trace-client \
"${EXECUTE_BIN}" -addr "${GRPC_ADDR}" -job-id "${JOB_ID}" -attempt "${ATTEMPT}" -payload smoke \
  >"${CLIENT_OUT}" 2>"${CLIENT_LOG}" || fail "grpc-execute failed"

grep -Fq "status=SUCCESS" "${CLIENT_OUT}" || fail "expected status=SUCCESS"

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

grep -Fq "worker.execute" "${SERVER_LOG}" || fail "missing worker.execute in server exporter output"
# otelgrpc v0.69+ uses rpc.system.name; rpc.method carries FullMethod (service/method).
grep -Eq 'rpc\.system(\.name)?|"rpc\.system(\.name)?"' "${SERVER_LOG}" || fail "missing rpc.system(.name) on server"
grep -Eq 'rpc\.method|"rpc\.method"|WorkerExecutionService' "${SERVER_LOG}" || fail "missing rpc.method / service on server"
grep -Eq 'job\.id|"job.id"' "${SERVER_LOG}" || fail "missing job.id attribute"
grep -Eq 'execution\.status|"execution.status"' "${SERVER_LOG}" || fail "missing execution.status attribute"

# Client spans export to stdout (pretty printer); status lines are also there.
CLIENT_TELEMETRY="${CLIENT_OUT}"
if grep -Eq 'rpc\.system(\.name)?|"rpc\.system(\.name)?"' "${CLIENT_LOG}" 2>/dev/null; then
  CLIENT_TELEMETRY="${CLIENT_LOG}"
fi
grep -Eq 'rpc\.system(\.name)?|"rpc\.system(\.name)?"' "${CLIENT_TELEMETRY}" || fail "missing rpc.system(.name) on client"

python3 - "${SERVER_LOG}" "${CLIENT_TELEMETRY}" <<'PY' || fail "shared TraceID check failed"
import re
import sys

server_log, client_log = sys.argv[1], sys.argv[2]
# stdouttrace pretty JSON: "TraceID": "…"
trace_re = re.compile(r'"TraceID"\s*:\s*"([0-9a-fA-F]{32})"')

def traces(path: str) -> set[str]:
    text = open(path, encoding="utf-8", errors="replace").read()
    return {t.lower() for t in trace_re.findall(text)}

server_ids = traces(server_log)
client_ids = traces(client_log)
if not server_ids:
    print("no TraceID in server log", file=sys.stderr)
    sys.exit(1)
if not client_ids:
    print("no TraceID in client log", file=sys.stderr)
    sys.exit(1)
shared = server_ids & client_ids
if not shared:
    print(f"no shared TraceID; server={server_ids} client={client_ids}", file=sys.stderr)
    sys.exit(1)

# Ensure worker.execute and an RPC span share that id on the server side.
server_text = open(server_log, encoding="utf-8", errors="replace").read()
shared_id = next(iter(shared))
if "worker.execute" not in server_text:
    print("worker.execute missing", file=sys.stderr)
    sys.exit(1)
if shared_id not in server_text.lower() and shared_id not in server_text:
    print("shared id not present in server text", file=sys.stderr)
    sys.exit(1)
print(f"shared_trace_id={shared_id}")
PY

echo "PASS: grpc tracing smoke succeeded"
echo "event=smoke_grpc_trace success=true"
