#!/usr/bin/env bash
#
# Educational smoke (Day 115): demonstrate the claim-before-completion gap.
# Claim an execution key, do NOT execute, claim again → second fails → recovery needed.
#
# Run from the repository root:
#   ./worker/scripts/smoke_execution_claim_gap.sh
#
# Requires Docker Redis (kernelq-redis). No Kafka. No worker execution.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

cd "${REPO_ROOT}"

fail() {
  echo "FAIL: $*" >&2
  echo "event=smoke_execution_claim_gap success=false" >&2
  exit 1
}

if ! docker compose ps --status running --services 2>/dev/null | grep -qx redis; then
  echo "starting redis..."
  docker compose up -d redis
fi

for _ in $(seq 1 30); do
  if docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
    break
  fi
  sleep 0.2
done

if ! docker exec kernelq-redis redis-cli ping 2>/dev/null | grep -q PONG; then
  fail "redis not ready (kernelq-redis)"
fi

JOB_ID="day115-claim-gap-$(date +%s)"
ATTEMPT=0
NAMESPACE="kernelq:idempotency"
REDIS_KEY="${NAMESPACE}:execution:${JOB_ID}:${ATTEMPT}"

# Clean slate for this unique key (usually a no-op).
docker exec kernelq-redis redis-cli DEL "${REDIS_KEY}" >/dev/null

echo "==> Simulating TryClaim then crash before Execute (job_id=${JOB_ID})..."
# First claim succeeds — mirrors Redis SET NX EX before executor runs.
FIRST_RAW="$(docker exec kernelq-redis redis-cli SET "${REDIS_KEY}" "1" NX EX 3600)"
# "Crash": we exit the execution path without running any worker/executor.

echo "==> Simulating replay TryClaim after crash (same key)..."
SECOND_RAW="$(docker exec kernelq-redis redis-cli SET "${REDIS_KEY}" "1" NX EX 3600)"

# redis-cli prints OK on success; empty / (nil) when NX loses.
if [[ "${FIRST_RAW}" == "OK" ]]; then
  FIRST_CLAIM="true"
else
  FIRST_CLAIM="false"
fi

if [[ -z "${SECOND_RAW}" || "${SECOND_RAW}" == "(nil)" ]]; then
  SECOND_CLAIM="false"
else
  SECOND_CLAIM="true"
fi

# Claim exists, no execution and no result → automatic recovery would be required.
if [[ "${FIRST_CLAIM}" == "true" && "${SECOND_CLAIM}" == "false" ]]; then
  RECOVERY_NEEDED="true"
else
  RECOVERY_NEEDED="false"
fi

echo "first_claim=${FIRST_CLAIM}"
echo "second_claim=${SECOND_CLAIM}"
echo "recovery_needed=${RECOVERY_NEEDED}"

docker exec kernelq-redis redis-cli DEL "${REDIS_KEY}" >/dev/null || true

if [[ "${FIRST_CLAIM}" != "true" || "${SECOND_CLAIM}" != "false" || "${RECOVERY_NEEDED}" != "true" ]]; then
  fail "expected first_claim=true second_claim=false recovery_needed=true"
fi

echo "PASS: execution claim gap demonstrated"
echo "event=smoke_execution_claim_gap success=true"
