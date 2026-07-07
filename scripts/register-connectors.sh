#!/usr/bin/env bash
# Register Kafka Connect connectors (Debezium Postgres source) — idempotent.
# Uses PUT /connectors/<name>/config so re-running updates the config in place.
set -euo pipefail

cd "$(dirname "$0")/.."

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECTOR_FILE="${1:-infra/debezium/postgres-source.json}"

name="$(jq -r '.name' "${CONNECTOR_FILE}")"
config="$(jq -c '.config' "${CONNECTOR_FILE}")"

echo "==> Waiting for Kafka Connect at ${CONNECT_URL}"
for _ in $(seq 1 30); do
  if curl -sf "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    break
  fi
  sleep 3
done

echo "==> Registering connector '${name}'"
curl -sf -X PUT \
  -H "Content-Type: application/json" \
  --data "${config}" \
  "${CONNECT_URL}/connectors/${name}/config" >/dev/null

echo "==> Connector status:"
curl -sf "${CONNECT_URL}/connectors/${name}/status" | jq -r '.name + " -> " + .connector.state'
echo "Done."
