#!/usr/bin/env bash
#
# Structured logging smoke (Day 127).
# Runs Go logging unit tests, logging-smoke helper, and Python logging tests.
# Uses temporary files and traps; does not call CloudWatch or AWS.
#
# Run from repository root:
#   ./worker/scripts/smoke_logging.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/internal/logging ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

ROOT_DIR="$(pwd)"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-logging-smoke-$$"
LOG_OUT="${TMP_DIR}/logs.jsonl"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_logging success=false" >&2
  exit 1
}

cleanup() {
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v go >/dev/null 2>&1 || fail "go is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"
command -v git >/dev/null 2>&1 || fail "git is required"
mkdir -p "${TMP_DIR}"

git status --short >"${STATUS_BEFORE}"

echo "==> Go logging unit tests..."
(
  cd "${ROOT_DIR}/worker"
  go test ./internal/logging -count=1
) || fail "go test ./internal/logging failed"

echo "==> logging-smoke helper (JSON + trace correlation)..."
(
  cd "${ROOT_DIR}/worker"
  go run ./cmd/logging-smoke >"${LOG_OUT}"
) || fail "logging-smoke helper failed"

[[ -s "${LOG_OUT}" ]] || fail "empty log output"

echo "==> Validating JSON lines and required fields..."
python3 - "${LOG_OUT}" <<'PY' || fail "log validation failed"
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
required = {"timestamp", "level", "message", "service", "environment", "version"}
forbidden = {"authorization", "password", "token", "raw_payload"}
found_trace = False
found_job = False

for i, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
    line = line.strip()
    if not line:
        continue
    try:
        entry = json.loads(line)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"line {i} not valid JSON: {exc}") from exc
    missing = required - set(entry)
    if missing:
        raise SystemExit(f"line {i} missing {missing}: {entry}")
    if forbidden & set(entry):
        raise SystemExit(f"line {i} has forbidden fields: {forbidden & set(entry)}")
    if entry.get("trace_id") and entry.get("span_id"):
        found_trace = True
    if entry.get("job_id") == "job-smoke-127" and entry.get("attempt") == 1:
        found_job = True

if not found_trace:
    raise SystemExit("no line with both trace_id and span_id")
if not found_job:
    raise SystemExit("no line with job_id/attempt for smoke job")
print("JSON validation ok")
PY

echo "==> Python logging tests..."
PYTHONPATH="${ROOT_DIR}" python3 -m pytest \
  "${ROOT_DIR}/control_plane/tests/test_logging_utils.py" \
  "${ROOT_DIR}/control_plane/tests/test_logging_context.py" \
  -q || fail "Python logging tests failed"

git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  fail "tracked/working tree changed during smoke (unexpected mutation)"
fi

echo "PASS: structured logging smoke succeeded"
echo "event=smoke_logging success=true"
