# Data contract — `events.deduplicated.v1`

**Producer:** Flink Job 2 `event-deduplicator`
([`flink/jobs/event-deduplicator`](../../flink/jobs/event-deduplicator/)).
**Consumers:** Flink Job 4 `event-geo-correlator` (Phase 3) → `risk.impact_candidates.v1`.
**Related:** [ADR-002](../adr/0002-flink-sql-vs-datastream.md),
[ADR-006](../adr/0006-event-envelope-and-schema-evolution.md),
[ADR-007](../adr/0007-idempotency-strategy.md), PLAN §11.2 / §18 Job 2.

## Flow

```
events.normalized.v1 ─▶ keyBy(source + source_event_id) ─▶ event-deduplicator ─▶ events.deduplicated.v1
                                                                 │  keyed state (+TTL, checkpointed)
                                                                 └─ replay → dropped
```

## Topic

| Topic | Key | cleanup.policy | Retention |
|---|---|---|---|
| `events.deduplicated.v1` | `source_event_id` | delete | PLAN §10.3 |

## Value

Structurally identical to [`events.normalized.v1`](events-normalized.md) (same envelope +
`NormalizedPayload` shape); Job 2 does not reshape records, it only decides whether to re-emit them.
See that contract for the field list.

## Semantics (PLAN §11.2)

State per key `source + source_event_id` = `{ payloadHash, sourceVersion, lastSeenAt }`. On each
incoming event:

| Condition | Action |
|---|---|
| key unseen | **emit** (`EMIT_NEW`), store hash |
| content hash equals stored | **drop** (`DROP`) — exact replay; refresh TTL only |
| content hash differs | **emit update** (`EMIT_UPDATE`), store new hash |

- **Content hash** ([`PayloadHash`](../../flink/jobs/event-deduplicator/src/main/java/com/sentinelchain/flink/deduplicator/PayloadHash.java))
  covers `event_time` + every canonical payload field, and **excludes** `event_id`, `trace_id` and
  `source_version`. So a pure version bump with identical content is dropped, while a magnitude
  correction or a `status` flip to `cancelled` is re-emitted as an update.
- **Replay safety (ADR-007):** Job 1 is at-least-once, so it can re-emit on restart. The dedup
  state is **checkpointed**, so a replay after a Flink restart is still dropped — this is the "no
  duplicate alert on replay" acceptance (PLAN §30).
- **State TTL:** default 30 days (`--state-ttl-days` / `DEDUP_STATE_TTL_DAYS`), matching raw-topic
  retention, so state for events that can no longer be replayed is reclaimed. TTL refreshes on every
  sighting (`OnCreateAndWrite`).
- **Delivery:** at-least-once into the output topic; downstream consumers must remain idempotent.

## Metrics (PLAN §26)

Flink counters on the operator: `dedup_emitted_new`, `dedup_emitted_update`,
`dedup_dropped_duplicate`.

## Serialization

**Avro** via Confluent Schema Registry (ADR-001).

| | |
|---|---|
| Schema | [`schemas/avro/events.deduplicated.v1.avsc`](../../schemas/avro/events.deduplicated.v1.avsc) (source of truth) |
| Subject | `events.deduplicated.v1-value` (TopicNameStrategy) |
| Compatibility | `BACKWARD`, pinned by `make register-schemas` |
| Key | `source_event_id` as plain UTF-8 — no `-key` subject |

## Verifying

```bash
make up-full && make create-topics && make register-schemas
make build-flink && make submit-job1 && make submit-job2
make run-usgs

# Emit the same event twice (e.g. restart run-usgs): the second pass is dropped, so
# events.deduplicated.v1 gains no extra record. Watch dedup_dropped_duplicate rise in the Flink UI.
docker run --rm --network sentinelchain_default confluentinc/cp-schema-registry:7.6.1 \
  kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --topic events.deduplicated.v1 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property print.key=true --from-beginning --timeout-ms 8000
```
