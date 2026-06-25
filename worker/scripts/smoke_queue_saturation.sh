#!/usr/bin/env bash
#
# Smoke test: bounded worker queue saturation stats (no real Kafka).
#
# Run from the repository root:
#   ./worker/scripts/smoke_queue_saturation.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${WORKER_DIR}"
go test ./internal/worker -run TestWorkerQueueSaturation -v

echo "PASS: worker queue saturation smoke test succeeded"
echo "event=smoke_worker_queue_saturation success=true"
