# ADR-006: Event envelope and schema evolution

- **Status:** Accepted
- **Date:** 2026-07-01
- **Deciders:** Architecture

## Context

Events flow through many topics and services and must carry consistent routing, lineage and
observability metadata, independent of the domain payload. We also need a disciplined way to
evolve schemas without breaking existing consumers (PLAN §9, §36.10).

## Decision

Adopt the **common envelope** from PLAN §9, implemented as
`sentinelchain_common.envelope.EventEnvelope` (Pydantic v2, `extra="forbid"`), with the Avro
counterpart under `schemas/avro`. Key rules:

- `event_id` is unique per message; **deduplication keys on `source + source_event_id`**, not
  `event_id` (see `EventEnvelope.dedup_key()` and ADR-007).
- `source_version` is the monotonic marker used to detect updates; `payload_hash()` detects
  no-op re-emissions (PLAN §11.2).
- `event_time` is business time; `ingested_at` is system receipt time. **All datetimes are
  timezone-aware UTC** (PLAN §36.11) — naive datetimes are rejected by `time.to_utc`.
- `event_version` marks the envelope schema version; payload schemas evolve under Schema
  Registry **BACKWARD** compatibility (ADR-001). Adding optional fields is backward-compatible;
  removing/renaming requires a new major payload version and a topic `.vN` bump.
- `trace_id` propagates through HTTP and Kafka headers for distributed tracing (PLAN §26).

## Alternatives considered

- **Per-service ad-hoc envelopes** — rejected: inconsistent lineage/observability, no single
  contract.
- **Envelope == payload (no separation)** — rejected: couples routing metadata to domain
  schema evolution.

## Consequences

- Positive: uniform lineage/tracing; clean dedup/versioning; safe additive evolution.
- Negative: every payload change must consider compatibility and topic versioning.
- Follow-up: contract tests asserting producer/consumer compatibility per topic.
