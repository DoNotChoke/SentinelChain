# Architecture Decision Records

Records of significant architecture decisions. New technologies require an ADR before adoption
(PLAN §36.3). Use [`0000-template.md`](0000-template.md) for new records.

| ADR | Title | Status |
|---|---|---|
| [001](0001-kafka-serialization-format.md) | Kafka serialization format | Accepted |
| [002](0002-flink-sql-vs-datastream.md) | Flink SQL vs DataStream API | Accepted |
| [006](0006-event-envelope-and-schema-evolution.md) | Event envelope and schema evolution | Accepted |
| [007](0007-idempotency-strategy.md) | Idempotency strategy | Accepted |
| [009](0009-cdc-current-state-design.md) | CDC current-state design | Accepted |

Planned (written when their milestone begins): ADR-003 OpenSearch hybrid retrieval store,
ADR-004 chunking outside Flink SQL, ADR-005 rule-based risk model before ML, ADR-008 RAG
validation architecture, ADR-010 lakehouse adoption phase.
