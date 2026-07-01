#!/usr/bin/env bash
# Create the SentinelChain Kafka topics (PLAN §10).
# Idempotent: re-running skips topics that already exist.
set -euo pipefail

# Prevent Git Bash/MSYS from rewriting absolute container paths (e.g. /opt/kafka/...)
# into Windows paths. No-op on Linux/macOS.
export MSYS_NO_PATHCONV=1

KAFKA_CONTAINER="${KAFKA_CONTAINER:-sentinelchain-kafka-1}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-kafka:29092}"
RF="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
KT="/opt/kafka/bin/kafka-topics.sh"

# topic_name : partitions : cleanup_policy
TOPICS=(
  # External raw (PLAN §10.1)
  "ext.usgs.raw.v1:3:delete"
  "ext.gdacs.raw.v1:3:delete"
  "ext.gdelt.raw.v1:3:delete"
  "ext.firms.raw.v1:3:delete"
  "ext.sec.raw.v1:3:delete"
  # Canonical (PLAN §10.3)
  "events.normalized.v1:3:delete"
  "events.deduplicated.v1:3:delete"
  "events.classified.v1:3:delete"
  "events.entities.v1:3:delete"
  "events.geo_enriched.v1:3:delete"
  "events.current.v1:3:compact"
  # Operational current-state (compacted; CDC bronze topics are created by Debezium)
  "ops.suppliers.current.v1:3:compact"
  "ops.facilities.current.v1:3:compact"
  "ops.shipments.current.v1:3:compact"
  "ops.inventory.current.v1:3:compact"
  "ops.purchase_orders.current.v1:3:compact"
  # Risk (PLAN §10.4)
  "risk.impact_candidates.v1:3:delete"
  "risk.features.v1:3:delete"
  "risk.scores.v1:3:delete"
  "risk.alerts.v1:3:delete"
  "risk.alerts.current.v1:3:compact"
  "risk.alerts.retry.v1:3:delete"
  "risk.alerts.dlq.v1:3:delete"
  # RAG (PLAN §10.5)
  "rag.documents.raw.v1:3:delete"
  "rag.documents.chunks.v1:3:delete"
  "rag.documents.embeddings.v1:3:delete"
  "rag.queries.v1:3:delete"
  "rag.retrieval_results.v1:3:delete"
  "rag.tool_calls.v1:3:delete"
  "rag.llm_requests.v1:3:delete"
  "rag.responses.raw.v1:3:delete"
  "rag.responses.validated.v1:3:delete"
  "rag.responses.retry.v1:3:delete"
  "rag.responses.dlq.v1:3:delete"
  # Audit (PLAN §10.6)
  "audit.data_quality.v1:3:delete"
  "audit.model_inference.v1:3:delete"
  "audit.security.v1:3:delete"
  "audit.pipeline_metrics.v1:3:delete"
)

echo "Creating ${#TOPICS[@]} topics on ${BOOTSTRAP} (rf=${RF}) via ${KAFKA_CONTAINER}..."
for entry in "${TOPICS[@]}"; do
  IFS=":" read -r name partitions cleanup <<<"${entry}"
  docker exec "${KAFKA_CONTAINER}" "${KT}" \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor "${RF}" \
    --config "cleanup.policy=${cleanup}" >/dev/null
  echo "  ✓ ${name} (partitions=${partitions}, cleanup=${cleanup})"
done

echo "Done."
