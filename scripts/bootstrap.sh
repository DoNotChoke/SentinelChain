#!/usr/bin/env bash
# One-shot local bootstrap: bring up core infra, wait for readiness, create topics and
# register schemas. Safe to re-run.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> Starting core infrastructure"
docker compose --profile core up -d

echo "==> Waiting for Kafka to become healthy"
for _ in $(seq 1 30); do
  status="$(docker inspect -f '{{.State.Health.Status}}' sentinelchain-kafka-1 2>/dev/null || echo starting)"
  if [ "${status}" = "healthy" ]; then
    echo "    Kafka is healthy"
    break
  fi
  sleep 5
done

echo "==> Creating topics"
bash scripts/create-topics.sh

echo "==> Registering schemas"
bash scripts/register-schemas.sh

echo "==> Registering Debezium CDC connector"
bash scripts/register-connectors.sh

echo "==> Bootstrap complete"
