# ADR-001: Kafka serialization format

- **Status:** Accepted
- **Date:** 2026-07-01
- **Deciders:** Architecture

## Context

Every event crossing Kafka needs a serialization format. Requirements (PLAN §2, §36.10):
schema evolution, compatibility enforcement, contract tests between producers/consumers, and
a "production-like" footprint that demonstrates data-engineering maturity. The format affects
every service and Flink job, so it must be decided before any topic carries traffic.

## Decision

Use **Apache Avro with Confluent Schema Registry** as the canonical wire format for all
business topics. Schemas live under `schemas/avro/*.avsc` (source of truth), are registered
with subject `\<topic\>-value` via `scripts/register-schemas.sh`, and are enforced with
**BACKWARD** compatibility by default.

- The shared :class:`EventEnvelope` (PLAN §9) is the outer structure; domain payloads are
  nested Avro records.
- **CDC exception (still in force):** Debezium CDC bronze topics (`ops.public.*`) use **Debezium
  JSON** (`schemas.enable=false`), and Flink Job 3 emits **JSON** current-state topics via
  `upsert-kafka` (see ADR-009). This keeps Job 3 as pure Flink SQL with no Avro/Registry wiring.
  Bronze CDC is a faithful copy of the source changelog, so JSON there costs little. Migrating the
  `ops.*.current.v1` topics to Avro is deferred to Milestone 3, when the Flink image gains the
  `flink-sql-avro-confluent-registry` jar for Jobs 1/2/4 anyway.
- As of Milestone 2b the canonical/external topics (`ext.usgs.raw.v1`, `audit.data_quality.v1`)
  are Avro.
- **Producers do not auto-register schemas.** `AvroEnvelopeSerializer` looks the schema up in the
  registry and fails if it is absent, so the registry — not a running producer — is the gate a
  schema change must pass. `scripts/register-schemas.sh` is the only path that creates a subject
  version, and it pins the subject to BACKWARD first.
- Message **keys** are plain UTF-8 (the deterministic partition key, e.g. `source_event_id`), not
  Avro; only `<topic>-value` subjects exist.

## Alternatives considered

- **JSON Schema** — easiest to start, human-readable, but weaker tooling for compatibility
  enforcement and larger payloads; less compelling as a portfolio signal.
- **Protobuf** — compact and strongly typed, good gRPC story, but Schema Registry + Flink +
  Debezium ergonomics are smoother with Avro in the Kafka ecosystem.

## Consequences

- Positive: enforced schema evolution, contract tests, compact payloads, idiomatic Kafka/Flink.
- Negative: extra tooling (Schema Registry must be up; codegen/serde wiring); steeper local
  setup.
- Local note: a single Kafka broker (KRaft) is used in the `core`/`full` compose profiles;
  3-broker replication is a `prod` concern. Replication factor is therefore 1 locally.
- Because Avro has no "any" type, a payload field holding arbitrary shapes (the offending record
  on `audit.data_quality.v1`) is carried as a JSON-encoded string. Acceptable there precisely
  because that topic must accept malformed input; it must **not** become a habit on business
  topics, where the payload is a modelled record.
- Implemented in `sentinelchain_common.avro` (serde) with contract tests in `tests/contract`.
