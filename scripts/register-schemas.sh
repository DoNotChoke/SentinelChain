#!/usr/bin/env bash
# Register Avro schemas (schemas/avro/*.avsc) with the Schema Registry (ADR-001).
# Subject naming: TopicNameStrategy -> "<topic>-value". Schemas are added as they are
# defined in later milestones; this script is a safe no-op until then.
set -euo pipefail

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMA_DIR="${SCHEMA_DIR:-schemas/avro}"

if [ ! -d "${SCHEMA_DIR}" ] || [ -z "$(ls -A "${SCHEMA_DIR}" 2>/dev/null || true)" ]; then
  echo "No Avro schemas in ${SCHEMA_DIR} yet — nothing to register."
  exit 0
fi

shopt -s nullglob
for schema_file in "${SCHEMA_DIR}"/*.avsc; do
  # Filename convention: <topic>.avsc  ->  subject <topic>-value
  topic="$(basename "${schema_file}" .avsc)"
  subject="${topic}-value"
  payload="$(jq -c '{schema: (. | tostring)}' "${schema_file}")"
  echo "Registering ${subject} from ${schema_file}..."
  curl -sf -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "${payload}" \
    "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions" >/dev/null
  echo "  ✓ ${subject}"
done

echo "Done."
