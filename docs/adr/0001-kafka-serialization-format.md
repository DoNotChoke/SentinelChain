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
- Debezium CDC bronze topics keep Debezium's own JSON/Avro envelope; Flink produces cleaned
  Avro current-state topics (see ADR-009).
- During very early bootstrap, the minimal producer in `libs/common` emits JSON; it is
  replaced by an Avro serializer wired to Schema Registry before Milestone 2 ships.

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
- Follow-up: add Avro serde helpers to `libs/common` and a `tests/contract` suite.
