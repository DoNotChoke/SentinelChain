# ADR-009: CDC current-state design

- **Status:** Accepted
- **Date:** 2026-07-01
- **Deciders:** Architecture

## Context

The risk pipeline joins events against the *current* state of suppliers, facilities,
shipments, inventory and purchase orders. That state lives in PostgreSQL and changes
continuously (PLAN §7.6). Downstream joins (Jobs 4/5) need an always-current, replay-safe view
keyed by entity id, derived from change data capture (PLAN §11.5, §18 Job 3).

## Decision

- **Debezium** captures row changes via Postgres logical decoding (`wal_level=logical`,
  `REPLICA IDENTITY FULL` on CDC tables) into bronze topics `ops.public.*` keyed by the table
  primary key.
- **Flink Job 3 `operational-current-state`** unwraps the Debezium envelope (handling
  `op=c/u/d`), normalizes timestamps to UTC (Debezium emits epoch micros), and writes one
  **compacted** current-state topic per entity: `ops.<entity>.current.v1`, keyed by entity id.
- **Deletes** become **tombstones** (null value) on the compacted topic so consumers and log
  compaction can drop the entity.
- Composite-key tables (e.g. `inventory` keyed by `warehouse_id + product_id`) use a composite
  message key.
- Consumers (Flink jobs, read-model builders) materialize current state from the compacted
  topic; on restart they rebuild from the compacted log — no state is lost (PLAN §30).

## Alternatives considered

- **Query Postgres directly at join time** — rejected: couples the stream pipeline to OLTP,
  adds latency and load, and is not replay-friendly.
- **Keep only raw CDC (no current-state topic)** — rejected: every consumer would re-implement
  envelope unwrapping and last-write-wins materialization.

## Consequences

- Positive: clean, compacted, replay-safe current state; unwrap logic written once.
- Negative: an extra Flink job and topic per entity; tombstone semantics must be tested.
- Follow-up: integration test — Postgres UPDATE/DELETE appears correctly on the current-state
  topic (PLAN §29, Milestone 1 done-criteria).
