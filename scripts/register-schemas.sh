#!/usr/bin/env bash
# Register Avro schemas (schemas/avro/*.avsc) with the Schema Registry (ADR-001).
# Subject naming: TopicNameStrategy -> "<topic>-value". Schemas are added as they are
# defined in later milestones; this script is a safe no-op until then.
set -euo pipefail

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMA_DIR="${SCHEMA_DIR:-schemas/avro}"
COMPATIBILITY="${SCHEMA_COMPATIBILITY:-BACKWARD}"
# The registry wants the schema as a JSON *string* inside a JSON object. Python does that
# quoting; jq is not a documented prerequisite of this repo, Python 3.12 is.
PYTHON="${PYTHON:-python}"

if [ ! -d "${SCHEMA_DIR}" ] || [ -z "$(ls -A "${SCHEMA_DIR}" 2>/dev/null || true)" ]; then
  echo "No Avro schemas in ${SCHEMA_DIR} yet — nothing to register."
  exit 0
fi

shopt -s nullglob
for schema_file in "${SCHEMA_DIR}"/*.avsc; do
  # Filename convention: <topic>.avsc  ->  subject <topic>-value
  topic="$(basename "${schema_file}" .avsc)"
  subject="${topic}-value"
  payload="$(SCHEMA_FILE="${schema_file}" "${PYTHON}" -c '
import json, os, pathlib
text = pathlib.Path(os.environ["SCHEMA_FILE"]).read_text(encoding="utf-8")
json.loads(text)  # fail early on a malformed .avsc
print(json.dumps({"schema": text}))
')"

  # Pin the subject's compatibility level BEFORE registering, so the very first evolution is
  # already governed by it (ADR-001). Subject-level config may be set before the subject exists.
  curl -sf -X PUT \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "{\"compatibility\": \"${COMPATIBILITY}\"}" \
    "${SCHEMA_REGISTRY_URL}/config/${subject}" >/dev/null

  echo "Registering ${subject} from ${schema_file}..."
  # Re-registering an identical schema is a no-op returning the existing id; an INCOMPATIBLE
  # change is rejected with HTTP 409 and `curl -f` fails the script — that is the contract gate.
  curl -sf -X POST \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "${payload}" \
    "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions" >/dev/null
  echo "  ✓ ${subject} (compatibility=${COMPATIBILITY})"
done

echo "Done."
