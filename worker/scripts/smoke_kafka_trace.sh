#!/usr/bin/env bash
#
# Smoke test: Kafka W3C trace propagation (Day 122).
# Instrumented dispatch publish → consumer kafka.process → worker.execute → result publish.
#
# Run from the repository root:
#   ./worker/scripts/smoke_kafka_trace.sh

set -euo pipefail

if [[ ! -f docker-compose.yml ]] || [[ ! -d worker/cmd/consumer ]]; then
  echo "ERROR: Run this script from the repository root." >&2
  exit 1
fi

KAFKA_CONTAINER="kernelq-kafka"
BOOTSTRAP_SERVER="kafka:29092"
CONSUMER_BIN="${TMPDIR:-/tmp}/kernelq-consumer-kafka-trace"
PUBLISH_BIN="${TMPDIR:-/tmp}/kernelq-dispatch-publish-kafka-trace"
WORKER_LOG="${TMPDIR:-/tmp}/kernelq-kafka-trace-worker.log"
PUBLISH_LOG="${TMPDIR:-/tmp}/kernelq-kafka-trace-publish.log"
WORKER_PID=""

RUN_ID="$(date +%s)"
JOB_ID="day122-trace-${RUN_ID}"
GROUP_ID="kernelq-smoke-kafka-trace-${RUN_ID}"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_kafka_trace success=false" >&2
  if [[ -f "${WORKER_LOG}" ]]; then
    echo "" >&2
    echo "=== Worker logs (${WORKER_LOG}) ===" >&2
    cat "${WORKER_LOG}" >&2
  fi
  if [[ -f "${PUBLISH_LOG}" ]]; then
    echo "" >&2
    echo "=== Publish logs (${PUBLISH_LOG}) ===" >&2
    cat "${PUBLISH_LOG}" >&2
  fi
  exit 1
}

cleanup() {
  if [[ -n "${WORKER_PID}" ]] && kill -0 "${WORKER_PID}" 2>/dev/null; then
    kill -INT "${WORKER_PID}" 2>/dev/null || true
    wait "${WORKER_PID}" 2>/dev/null || true
    WORKER_PID=""
  fi
}

trap cleanup EXIT INT TERM

echo "==> Ensuring Kafka is running..."
if ! docker compose ps --status running --services 2>/dev/null | grep -qx kafka; then
  docker compose up -d zookeeper kafka
fi

for _ in $(seq 1 60); do
  if docker exec "${KAFKA_CONTAINER}" kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done
if ! docker exec "${KAFKA_CONTAINER}" kafka-topics --bootstrap-server "${BOOTSTRAP_SERVER}" --list >/dev/null 2>&1; then
  fail "kafka not ready"
fi

echo "==> Building consumer and dispatch-publish..."
(
  cd worker
  go build -o "${CONSUMER_BIN}" ./cmd/consumer
  go build -o "${PUBLISH_BIN}" ./cmd/dispatch-publish
)

: >"${WORKER_LOG}"
: >"${PUBLISH_LOG}"

echo "==> Starting instrumented worker consumer..."
KERNELQ_KAFKA_GROUP_ID="${GROUP_ID}" \
KERNELQ_KAFKA_AUTO_OFFSET_RESET=latest \
KERNELQ_WORKER_IDEMPOTENCY_BACKEND=memory \
KERNELQ_OTEL_ENABLED=true \
KERNELQ_OTEL_EXPORTER=stdout \
KERNELQ_OTEL_SERVICE_NAME=kernelq-kafka-trace-worker \
"${CONSUMER_BIN}" >"${WORKER_LOG}" 2>&1 &
WORKER_PID=$!

READY=0
for _ in $(seq 1 50); do
  if grep -Fq "KernelQ worker consumer started" "${WORKER_LOG}" 2>/dev/null; then
    READY=1
    break
  fi
  if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
    fail "worker exited before becoming ready"
  fi
  sleep 0.1
done
[[ "${READY}" -eq 1 ]] || fail "worker did not start"
# Allow consumer group join before publish.
for _ in $(seq 1 30); do
  if grep -Eq 'otel_tracer_provider_start|kafka_group_id=' "${WORKER_LOG}" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
sleep 1

echo "==> Publishing instrumented dispatch for ${JOB_ID}..."
KERNELQ_OTEL_ENABLED=true \
KERNELQ_OTEL_EXPORTER=stdout \
KERNELQ_OTEL_SERVICE_NAME=kernelq-kafka-trace-publisher \
"${PUBLISH_BIN}" -job-id "${JOB_ID}" -payload smoke >"${PUBLISH_LOG}" 2>&1 \
  || fail "dispatch-publish failed"

grep -Fq "event=dispatch_published" "${PUBLISH_LOG}" || fail "missing dispatch_published"

echo "==> Waiting for worker execution..."
SEEN=0
for _ in $(seq 1 80); do
  if grep -Fq "received task job_id=${JOB_ID}" "${WORKER_LOG}" 2>/dev/null; then
    SEEN=1
    break
  fi
  if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
    fail "worker exited before processing job"
  fi
  sleep 0.25
done
[[ "${SEEN}" -eq 1 ]] || fail "worker did not process job ${JOB_ID}"

echo "==> Stopping worker to flush spans..."
kill -INT "${WORKER_PID}" 2>/dev/null || true
for _ in $(seq 1 50); do
  if ! kill -0 "${WORKER_PID}" 2>/dev/null; then
    break
  fi
  sleep 0.1
done
wait "${WORKER_PID}" 2>/dev/null || true
WORKER_PID=""

grep -Fq "kafka.publish" "${PUBLISH_LOG}" || fail "missing kafka.publish in publisher output"
grep -Fq "kafka.process" "${WORKER_LOG}" || fail "missing kafka.process in worker output"
grep -Fq "worker.execute" "${WORKER_LOG}" || fail "missing worker.execute in worker output"
grep -Fq "kafka.publish" "${WORKER_LOG}" || fail "missing result kafka.publish in worker output"
grep -Eq 'job\.id|"job.id"' "${WORKER_LOG}" || fail "missing job.id"
# Payload bodies must not appear as span attributes.
if grep -Eq '"Key": "payload"|messaging\.payload|event\.payload' "${WORKER_LOG}" "${PUBLISH_LOG}"; then
  fail "payload appears to be traced as an attribute"
fi

python3 - "${PUBLISH_LOG}" "${WORKER_LOG}" "${JOB_ID}" <<'PY' || fail "shared TraceID check failed"
import re
import sys

publish_log, worker_log, job_id = sys.argv[1], sys.argv[2], sys.argv[3]
trace_re = re.compile(r'"TraceID"\s*:\s*"([0-9a-fA-F]{32})"')

def traces(path: str) -> set[str]:
    text = open(path, encoding="utf-8", errors="replace").read()
    return {t.lower() for t in trace_re.findall(text)}

pub_ids = traces(publish_log)
worker_ids = traces(worker_log)
if not pub_ids:
    print("no TraceID in publisher log", file=sys.stderr)
    sys.exit(1)
if not worker_ids:
    print("no TraceID in worker log", file=sys.stderr)
    sys.exit(1)
shared = pub_ids & worker_ids
if not shared:
    print(f"no shared TraceID; publish={pub_ids} worker={worker_ids}", file=sys.stderr)
    sys.exit(1)

worker_text = open(worker_log, encoding="utf-8", errors="replace").read()
for name in ("kafka.process", "worker.execute", "kafka.publish"):
    if name not in worker_text:
        print(f"missing {name} in worker log", file=sys.stderr)
        sys.exit(1)
if job_id not in worker_text:
    print("job id not present in worker log", file=sys.stderr)
    sys.exit(1)
print(f"shared_trace_id={next(iter(shared))}")
PY

echo "PASS: kafka trace smoke succeeded"
echo "event=smoke_kafka_trace success=true"
