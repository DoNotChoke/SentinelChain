# ADR-002: Flink SQL vs DataStream API

- **Status:** Accepted
- **Date:** 2026-07-01
- **Deciders:** Architecture

## Context

The risk pipeline is a set of Flink jobs (PLAN §18). They differ widely in complexity: some
are stateless schema mapping; others need keyed state with custom TTL, version comparison,
broadcast joins against slowly-changing facility state, and precise late-event handling.
Choosing one API for everything would either over-complicate the simple jobs or hit
expressiveness limits on the hard ones.

## Decision

Use a **hybrid approach, picked per job by complexity**:

- **Flink SQL** for stateless / lightly-stateful jobs where relational operators suffice:
  `external-event-normalizer` (Job 1) and `operational-current-state` (Job 3, Debezium unwrap).
- **DataStream API (Java)** for jobs needing fine-grained state, custom triggers, broadcast
  state or bespoke late-event logic: `event-deduplicator` (Job 2), `event-geo-correlator`
  (Job 4), `risk-feature-builder` (Job 5), `alert-manager` (Job 7).
- `risk-scorer` (Job 6) starts as DataStream with rule-based logic; ML inference is added
  later (ADR-005).

Shared serde/config/test-harness utilities live in `flink/common` and `libs/common-java`.
Topic names are never hard-coded (PLAN §23) — they are injected via config.

## Alternatives considered

- **Pure Flink SQL** — fastest to write, but version-aware dedup and broadcast geo correlation
  are awkward or impossible without UDFs/changelog gymnastics.
- **Pure DataStream** — maximal control, but verbose and error-prone for simple mappings.

## Consequences

- Positive: each job uses the simplest API that fits; SQL where it shines, DataStream where
  control is needed.
- Negative: two programming models to maintain and test.
- Follow-up: operator test harness in `flink/common`; Testcontainers for Kafka/Postgres in
  integration tests (PLAN §29).
