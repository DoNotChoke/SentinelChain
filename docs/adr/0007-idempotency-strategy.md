# ADR-007: Idempotency strategy

- **Status:** Accepted
- **Date:** 2026-07-01
- **Deciders:** Architecture

## Context

A hard acceptance criterion is that **replay must not create duplicate alerts** and restarts
must not lose or duplicate work (PLAN §30, §33, §34: duplicate alert rate < 1%). Sources are
polled and may re-deliver; Kafka gives at-least-once by default; some side effects (embedding,
LLM, OpenSearch writes) are outside any Kafka transaction.

## Decision

Layered idempotency:

1. **Producers** enable idempotence with deterministic keys (`libs/common.kafka.producer_config`:
   `enable.idempotence=true`, `acks=all`). Keys are stable domain identifiers (e.g.
   `source_event_id`, `event_id+asset_id`, `alert_id`).
2. **Ingestion cursors** are committed **only after Kafka acknowledges** the produce (PLAN
   §11.1). The producer also suppresses no-op re-emissions using `EventEnvelope.payload_hash()`.
3. **Stream dedup** is performed in Flink Job 2 keyed on `source + source_event_id`, comparing
   `source_version` / payload hash (ADR-006, PLAN §11.2).
4. **Alert dedup** keys on `event_id + facility_id + alert_type` (PLAN §11.7); the alert
   manager updates rather than re-creates.
5. **External (non-Kafka) effects** use a deterministic `operation_id = hash(input_id +
   model_version + operation_type)` (`libs/common.resilience.operation_id`); processed
   operation ids are persisted so retries are no-ops.

Consumers commit offsets **only after successful processing** (`enable.auto.commit=false`).

## Alternatives considered

- **Rely on Kafka exactly-once (EOS) everywhere** — rejected: EOS does not cover external
  side effects (embeddings/LLM/OpenSearch), and full EOS adds latency/complexity.
- **Best-effort dedup at the sink only** — rejected: too late; duplicates already fan out.

## Consequences

- Positive: replay-safe end to end; meets the < 1% duplicate-alert target.
- Negative: requires an idempotency store for external operations and careful commit ordering.
- Follow-up: choose the idempotency store (Redis vs Postgres) per service in their ADRs/READMEs.
