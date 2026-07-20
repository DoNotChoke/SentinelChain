# Data contract — `events.normalized.v1`

**Producer:** Flink Job 1 `external-event-normalizer`
([`flink/jobs/external-event-normalizer`](../../flink/jobs/external-event-normalizer/)).
**Consumers:** Flink Job 2 `event-deduplicator` (Phase 3) → `events.deduplicated.v1`.
**Related:** [ADR-001](../adr/0001-kafka-serialization-format.md),
[ADR-002](../adr/0002-flink-sql-vs-datastream.md),
[ADR-006](../adr/0006-event-envelope-and-schema-evolution.md),
[ADR-007](../adr/0007-idempotency-strategy.md), PLAN §11.1 / §18 Job 1 / §28.

## Flow

```
ext.usgs.raw.v1 (Avro) ─▶ external-event-normalizer ─▶ events.normalized.v1
                                    └─ §28 rejects ──▶ audit.data_quality.v1
```

Job 1 is the first canonical stage: it collapses each source's raw shape into one
source-agnostic record carrying the geometry/severity every downstream job needs, so Job 2
(dedup), Job 4 (geo) and beyond never parse source-specific payloads again.

## Topic

| Topic | Key | cleanup.policy | Retention |
|---|---|---|---|
| `events.normalized.v1` | `source_event_id` | delete | PLAN §10.3 |

## Envelope

The value is the shared envelope (PLAN §9, ADR-006) with a canonical payload. Normalized values:

| Field | Value |
|---|---|
| `event_type` | canonical category, e.g. `earthquake` (drives §11.4 threshold rules) |
| `source` | upstream source, carried through (e.g. `usgs`) |
| `source_event_id` | stable across upstream updates; the dedup key with `source` |
| `source_version` | upstream `updated` time, ISO-8601 UTC — carried through for Job 2 versioning |
| `event_time` | business time, `timestamp-millis`; the watermarked event-time attribute |
| `ingested_at` | carried from the raw envelope |
| `normalized_at` | `timestamp-millis` — when Job 1 produced this record |
| `payload` | see below |

### `payload` (`NormalizedPayload`)

| Field | Type | Notes |
|---|---|---|
| `latitude` | double | WGS84, validated to `[-90, 90]` |
| `longitude` | double | WGS84, validated to `[-180, 180]` |
| `severity` | double \| null | canonical magnitude (USGS earthquake magnitude); null if upstream has none |
| `severity_scale` | string \| null | unit of `severity`, e.g. `magnitude_mw`; null when `severity` is null |
| `depth_km` | double \| null | carried from source |
| `place` | string \| null | human-readable location |
| `source_url` | string \| null | carried from source |
| `status` | string | `active` \| `cancelled`; Job 2 flips this on an upstream cancellation (§11.2) |

## Semantics

- **Canonicalization:** `event_type` becomes the canonical category (`usgs.earthquake.raw` →
  `earthquake`); `magnitude` maps to `severity` + `severity_scale=magnitude_mw`.
- **Event time & watermarks:** the watermark is `BoundedOutOfOrderness(5m)` on `event_time`, with
  1-minute idleness so an idle partition does not stall downstream time. `event_time` /
  `ingested_at` pass through unchanged; only `normalized_at` is added.
- **Data quality (PLAN §28):** a record failing any check (`latitude_out_of_range`,
  `longitude_out_of_range`, `magnitude_out_of_range`, `event_time_in_future`,
  `missing_source_event_id`) is routed to `audit.data_quality.v1` and is **not** emitted here.
  A null magnitude is allowed (very recent quakes have none) — only out-of-range values are flagged.
- **Delivery:** at-least-once. Exactly-once is provided downstream by Job 2, which keys state on
  `source + source_event_id` (ADR-007), so a replayed normalized record is idempotent.

## Serialization

**Avro** via Confluent Schema Registry (ADR-001).

| | |
|---|---|
| Schema | [`schemas/avro/events.normalized.v1.avsc`](../../schemas/avro/events.normalized.v1.avsc) (source of truth) |
| Subject | `events.normalized.v1-value` (TopicNameStrategy) |
| Compatibility | `BACKWARD`, pinned by `make register-schemas` |
| Key | `source_event_id` as plain UTF-8 — no `-key` subject |

`event_time` / `ingested_at` / `normalized_at` are Avro `timestamp-millis` (epoch-millis `long` on
the wire), matching the raw topic so Flink assigns watermarks without conversion.

## Verifying

```bash
make up-full && make create-topics && make register-schemas   # topics + subjects
make register-connectors && make seed                          # operational data (optional here)
make build-flink && make submit-job1                           # build + submit Job 1
make run-usgs                                                  # feed ext.usgs.raw.v1

# Decode the normalized output through the registry (Avro is binary):
docker run --rm --network sentinelchain_default confluentinc/cp-schema-registry:7.6.1 \
  kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --topic events.normalized.v1 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property print.key=true --from-beginning --timeout-ms 8000
```
