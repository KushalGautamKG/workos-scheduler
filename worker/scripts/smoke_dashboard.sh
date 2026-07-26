#!/usr/bin/env bash
#
# Grafana dashboard smoke (Day 128).
# Offline JSON validation — does not start Grafana.
#
# Run from repository root:
#   ./worker/scripts/smoke_dashboard.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -f deploy/observability/grafana/kernelq-dashboard.json ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

DASHBOARD="deploy/observability/grafana/kernelq-dashboard.json"
TMP_DIR="${TMPDIR:-/tmp}/kernelq-dashboard-smoke-$$"
STATUS_BEFORE="${TMP_DIR}/git-before.txt"
STATUS_AFTER="${TMP_DIR}/git-after.txt"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_dashboard success=false" >&2
  exit 1
}

cleanup() {
  rm -rf "${TMP_DIR}"
}

trap cleanup EXIT INT TERM

command -v python3 >/dev/null 2>&1 || fail "python3 is required"
command -v git >/dev/null 2>&1 || fail "git is required"
mkdir -p "${TMP_DIR}"

git status --short >"${STATUS_BEFORE}"

echo "==> Validating dashboard JSON..."
python3 - "${DASHBOARD}" <<'PY' || fail "dashboard validation failed"
import json
import re
import sys
from pathlib import Path

path = Path(sys.argv[1])
data = json.loads(path.read_text(encoding="utf-8"))

title = data.get("title")
if not title:
    raise SystemExit("dashboard title missing")
if title != "KernelQ Monitoring":
    raise SystemExit(f"unexpected title: {title!r}")

panels = data.get("panels") or []
if not panels:
    raise SystemExit("no panels")

titles = []
exprs = []
for panel in panels:
    if panel.get("title"):
        titles.append(panel["title"])
    for target in panel.get("targets") or []:
        expr = target.get("expr")
        if expr:
            exprs.append(expr)
    ds = panel.get("datasource")
    if isinstance(ds, dict) and "uid" in ds:
        uid = str(ds["uid"])
        if uid and not uid.startswith("${") and uid not in {"Prometheus"}:
            # Allow templated ${datasource}; reject hardcoded UIDs.
            if re.fullmatch(r"[A-Za-z0-9_-]{8,}", uid) and "${" not in uid:
                raise SystemExit(f"hardcoded datasource uid: {uid}")

required_title_substrings = [
    "Worker availability",
    "Queue depth",
    "Jobs/sec",
    "Success",
    "Failure",
    "Retry",
    "Execution latency",
    "Queue latency",
    "Kafka publish",
    "Kafka consume",
    "Redis idempotency",
    "Top worker errors",
]
joined = " | ".join(titles)
for needle in required_title_substrings:
    if needle.lower() not in joined.lower():
        raise SystemExit(f"missing panel covering {needle!r}; have {titles}")

required_exprs = [
    "kernelq:worker_availability",
    "kernelq:queue_depth",
    "kernelq:jobs_per_second",
    "kernelq:job_success_rate",
    "kernelq:job_failure_rate",
    "kernelq:retry_rate",
    "kernelq:execution_latency_p95",
    "kernelq:queue_latency_p95",
    "kernelq_result_publish_total",
    "kernelq_kafka_messages_consumed_total",
    "kernelq_redis_idempotency",
    "kernelq_worker_errors_total",
]
blob = "\n".join(exprs)
for needle in required_exprs:
    if needle not in blob:
        raise SystemExit(f"missing Prometheus query covering {needle}")

text = path.read_text(encoding="utf-8")
if "uid\": \"${datasource}\"" not in text and '"uid": "${datasource}"' not in text:
    raise SystemExit("expected templated datasource uid ${datasource}")

forbidden_url_bits = [
    "https://grafana.",
    "https://prometheus.",
    "amazonaws.com",
    "grafana.prod",
]
for bit in forbidden_url_bits:
    if bit in text.lower():
        raise SystemExit(f"forbidden production URL fragment: {bit}")

# Templating datasource variable should exist.
templating = (data.get("templating") or {}).get("list") or []
if not any(t.get("name") == "datasource" for t in templating):
    raise SystemExit("datasource template variable missing")

print(f"title={title!r} panels={len(panels)} queries={len(exprs)} ok")
PY

git status --short >"${STATUS_AFTER}"
if ! diff -q "${STATUS_BEFORE}" "${STATUS_AFTER}" >/dev/null; then
  fail "tracked/working tree changed during smoke (unexpected mutation)"
fi

echo "PASS: dashboard smoke succeeded"
echo "event=smoke_dashboard success=true"
