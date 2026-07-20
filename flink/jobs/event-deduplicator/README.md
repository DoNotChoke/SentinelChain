# Flink Job 2 — `event-deduplicator`

Reads `events.normalized.v1`, keys by `source + source_event_id`, and re-emits to
`events.deduplicated.v1` only records whose content changed — dropping exact replays. First
**stateful** DataStream/Java job (ADR-002): keyed `ValueState` with TTL, checkpointed so the guard
survives a restart (the "no duplicate alert on replay" acceptance, ADR-007 / PLAN §30).

See the [data contract](../../../docs/data-contracts/events-deduplicated.md) for the full semantics.

## Layout

| File | Role |
|---|---|
| [`DeduplicatorJob.java`](src/main/java/com/sentinelchain/flink/deduplicator/DeduplicatorJob.java) | Wiring: source → `keyBy` → process → sink |
| [`DeduplicateFunction.java`](src/main/java/com/sentinelchain/flink/deduplicator/DeduplicateFunction.java) | `KeyedProcessFunction` holding `ValueState<DedupState>` + TTL + metrics |
| [`Deduplication.java`](src/main/java/com/sentinelchain/flink/deduplicator/Deduplication.java) | Pure decision `EMIT_NEW / DROP / EMIT_UPDATE` (unit-tested) |
| [`PayloadHash.java`](src/main/java/com/sentinelchain/flink/deduplicator/PayloadHash.java) | Deterministic content signature (unit-tested) |
| [`DedupState.java`](src/main/java/com/sentinelchain/flink/deduplicator/DedupState.java) | POJO kept in Flink managed state |

## Config

| Arg | Env | Default |
|---|---|---|
| `--bootstrap-servers` | `KAFKA_BOOTSTRAP_INTERNAL` | `kafka:29092` |
| `--schema-registry-url` | `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |
| `--consumer-group-id` | `CONSUMER_GROUP_ID` | `flink-event-deduplicator` |
| `--state-ttl-days` | `DEDUP_STATE_TTL_DAYS` | `30` |

## Build, run

```bash
make build-flink                         # → target/event-deduplicator.jar (runs unit tests)
make up-full && make create-topics register-schemas
make submit-job1 && make submit-job2     # normalize, then dedup
make run-usgs                            # restart it to prove replays are dropped
```

Metrics on the operator (Flink UI): `dedup_emitted_new`, `dedup_emitted_update`,
`dedup_dropped_duplicate`.
