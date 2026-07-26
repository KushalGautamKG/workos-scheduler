#!/usr/bin/env bash
#
# Monitoring configuration smoke (Day 128).
# Offline only — does not start Prometheus, Grafana, or alert delivery.
#
# Run from repository root:
#   ./worker/scripts/smoke_monitoring.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d deploy/observability/prometheus ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

ROOT_DIR="$(pwd)"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-monitoring-smoke-$$"
RENDERED="${TMP_DIR}/prometheus.yaml"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"

EXPECTED_RECORDINGS=(
  kernelq:job_success_rate
  kernelq:job_failure_rate
  kernelq:execution_latency_p95
  kernelq:queue_latency_p95
  kernelq:retry_rate
  kernelq:publish_success_rate
)

EXPECTED_ALERTS=(
  WorkerUnavailable
  HighExecutionLatency
  HighFailureRate
  RetryStorm
  PublishFailures
  KafkaConsumerStopped
  QueueBacklogGrowing
)

EXPECTED_RUNBOOKS=(
  docs/runbooks/high-latency.md
  docs/runbooks/high-error-rate.md
  docs/runbooks/kafka-backlog.md
  docs/runbooks/redis-failures.md
  docs/runbooks/worker-unavailable.md
  docs/runbooks/alerts.md
)

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_monitoring success=false" >&2
  exit 1
}

cleanup() {
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v kubectl >/dev/null 2>&1 || fail "kubectl is required"
command -v git >/dev/null 2>&1 || fail "git is required"
command -v python3 >/dev/null 2>&1 || fail "python3 is required"
mkdir -p "${TMP_DIR}"

git status --short >"${STATUS_BEFORE}"

echo "==> Rendering Prometheus monitoring resources..."
kubectl kustomize deploy/observability/prometheus >"${RENDERED}" \
  || fail "kustomize prometheus failed"

grep -Fq 'kernelq-prometheus-recording-rules' "${RENDERED}" || fail "recording rules ConfigMap missing"
grep -Fq 'kernelq-prometheus-alert-rules' "${RENDERED}" || fail "alert rules ConfigMap missing"

for name in "${EXPECTED_RECORDINGS[@]}"; do
  grep -Fq "${name}" "${RENDERED}" || fail "missing recording rule ${name}"
done

for name in "${EXPECTED_ALERTS[@]}"; do
  grep -Fq "alert: ${name}" "${RENDERED}" || fail "missing alert ${name}"
done

grep -Eq 'severity:[[:space:]]*(warning|critical)' "${RENDERED}" || fail "alert severities missing"
grep -Fq 'runbook:' "${RENDERED}" || fail "runbook annotations missing"
grep -Fq 'docs/runbooks/' "${RENDERED}" || fail "runbook paths missing"

[[ -f deploy/observability/grafana/kernelq-dashboard.json ]] \
  || fail "dashboard JSON missing"

for path in "${EXPECTED_RUNBOOKS[@]}"; do
  [[ -f "${ROOT_DIR}/${path}" ]] || fail "missing runbook ${path}"
done

# Resolve runbook annotations from rendered alerts against filesystem.
python3 - "${RENDERED}" "${ROOT_DIR}" <<'PY' || fail "runbook reference or duplicate alert check failed"
import re
import sys
from pathlib import Path

rendered = Path(sys.argv[1]).read_text(encoding="utf-8")
root = Path(sys.argv[2])

alerts = re.findall(r"(?m)^\s*- alert:\s*(\S+)\s*$", rendered)
if not alerts:
    raise SystemExit("no alerts found")
if len(alerts) != len(set(alerts)):
    dupes = sorted({a for a in alerts if alerts.count(a) > 1})
    raise SystemExit(f"duplicate alert names: {dupes}")

runbooks = re.findall(r"(?m)^\s*runbook:\s*(\S+)\s*$", rendered)
if not runbooks:
    raise SystemExit("no runbook annotations found")
for rel in runbooks:
    path = root / rel
    if not path.is_file():
        raise SystemExit(f"runbook does not resolve: {rel}")

severities = re.findall(r"(?m)^\s*severity:\s*(\S+)\s*$", rendered)
bad = [s for s in severities if s not in {"warning", "critical"}]
if bad:
    raise SystemExit(f"invalid severities: {bad}")

print(f"alerts={len(alerts)} runbooks={len(set(runbooks))} ok")
PY

git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  fail "tracked/working tree changed during smoke (unexpected mutation)"
fi

echo "PASS: monitoring smoke succeeded"
echo "event=smoke_monitoring success=true"
