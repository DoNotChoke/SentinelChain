# Data contract — `ext.usgs.raw.v1`

**Producer:** `ingestion-usgs` service ([`services/ingestion-usgs`](../../services/ingestion-usgs/))
**Consumers:** Flink Job 1 `external-event-normalizer` (Phase 3) → `events.normalized.v1`.
**Related:** [ADR-001](../adr/0001-kafka-serialization-format.md),
[ADR-006](../adr/0006-event-envelope-and-schema-evolution.md),
[ADR-007](../adr/0007-idempotency-strategy.md), PLAN §7.1 / §9 / §11.1 / §28.

## Flow

```
USGS GeoJSON feed → ingestion-usgs (poll + cursor dedup + idempotent producer) → ext.usgs.raw.v1
                                    └─ invalid records → audit.data_quality.v1
```

## Topic

| Topic | Key | cleanup.policy | Retention |
|---|---|---|---|
| `ext.usgs.raw.v1` | `source_event_id` (e.g. `us7000xxxx`) | delete | 30 days (PLAN §10.1) |

## Envelope

The value is the shared [`EventEnvelope`](../../libs/common/src/sentinelchain_common/envelope.py)
(PLAN §9). USGS-specific values:

| Field | Value |
|---|---|
| `event_type` | `usgs.earthquake.raw` |
| `source` | `usgs` |
| `source_event_id` | USGS event id (stable across upstream updates) |
| `source_version` | upstream `updated` time, ISO-8601 UTC — drives dedup/versioning |
| `event_time` | earthquake origin time (from `properties.time`) |
| `payload` | see below |

### `payload`

| Field | Type | Source |
|---|---|---|
| `source_event_id` | string | feature `id` |
| `magnitude` | number \| null | `properties.mag` |
| `latitude` | number | `geometry.coordinates[1]` |
| `longitude` | number | `geometry.coordinates[0]` |
| `depth_km` | number \| null | `geometry.coordinates[2]` |
| `place` | string \| null | `properties.place` |
| `event_time` | string (ISO-8601 UTC) | `properties.time` (epoch ms) |
| `updated_time` | string (ISO-8601 UTC) | `properties.updated` (epoch ms) |
| `source_url` | string \| null | `properties.url` |

### Example

```json
{
  "event_id": "…uuid…",
  "event_type": "usgs.earthquake.raw",
  "event_version": 1,
  "source": "usgs",
  "source_event_id": "us7000valid",
  "source_version": "2026-07-01T09:08:00Z",
  "event_time": "2026-07-01T09:00:00Z",
  "ingested_at": "2026-07-01T09:18:05Z",
  "trace_id": "…uuid…",
  "tenant_id": "demo",
  "payload": {
    "source_event_id": "us7000valid",
    "magnitude": 6.2,
    "latitude": 35.1,
    "longitude": 139.2,
    "depth_km": 38.0,
    "place": "Near the east coast of Honshu, Japan",
    "event_time": "2026-07-01T09:00:00Z",
    "updated_time": "2026-07-01T09:08:00Z",
    "source_url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000valid"
  }
}
```

## Semantics

- **Dedup / versioning (PLAN §11.1):** the producer emits an event only when it is new or when
  its marker `"<source_version>:<payload_hash>"` changed. A re-poll or a service **restart with
  an unchanged feed produces nothing** — the cursor lives in Redis, keyed by `source_event_id`,
  with a 30-day TTL matching topic retention.
- **Idempotent producer**, keyed by `source_event_id`. The cursor is committed **only after the
  broker acknowledges** the produce; a crash in between causes a safe re-emit (dedup drops it
  downstream) rather than a lost event.
- **Data quality (PLAN §28):** records failing lat/lon/magnitude/future-time/id checks are
  routed to `audit.data_quality.v1` (`event_type=data_quality.violation`) and are **not** emitted
  to this topic.

## Serialization

M2a: **JSON** envelope via the shared `EnvelopeProducer`. Avro + Schema Registry migration is
M2b (see ADR-001).

## Verifying

```bash
make up && make create-topics
python -m ingestion_usgs.main   # or: make run-usgs

# Observe raw events:
docker exec sentinelchain-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ext.usgs.raw.v1 \
  --from-beginning --timeout-ms 8000 --property print.key=true
```

Restarting the service must **not** re-emit unchanged events (dedup cursor in Redis).
