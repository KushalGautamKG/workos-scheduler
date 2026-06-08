#!/usr/bin/env bash
#
# Create KernelQ Kafka topics for local development.
#
# Prerequisites:
#   - Docker Compose stack is running (at least Zookeeper + Kafka).
#   - Container name: kernelq-kafka (see docker-compose.yml).
#
# Usage (from repo root):
#   ./infra/kafka/create-topics.sh
#
# Safe to rerun: each topic is created with --if-not-exists.

set -euo pipefail

# Kafka container started by docker compose (docker-compose.yml).
KAFKA_CONTAINER="kernelq-kafka"

# Bootstrap address *inside* the Docker network.
# Host clients use localhost:9092; commands run via docker exec use kafka:29092.
BOOTSTRAP_SERVER="kafka:29092"

# Topic layout for KernelQ job handoff:
#   dispatch — normal runnable work after scheduler claim (control plane → workers)
#   retry    — jobs that failed but should run again (backoff / retry policy)
#   dlq      — dead-letter queue for poison messages or permanently failed jobs
#   results  — worker execution outcomes back to the control plane (workers → control plane)
TOPICS=(
  "kernelq.jobs.dispatch"
  "kernelq.jobs.retry"
  "kernelq.jobs.dlq"
  "kernelq.jobs.results"
)

PARTITIONS=3
REPLICATION_FACTOR=1

echo "Creating KernelQ Kafka topics (container=${KAFKA_CONTAINER}, bootstrap=${BOOTSTRAP_SERVER})..."

for topic in "${TOPICS[@]}"; do
  echo "  -> ${topic}"
  docker exec "${KAFKA_CONTAINER}" kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION_FACTOR}"
done

echo ""
echo "All KernelQ topics (listing via kafka-topics --list):"
docker exec "${KAFKA_CONTAINER}" kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --list
