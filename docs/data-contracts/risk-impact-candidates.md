# Data contract — `risk.impact_candidates.v1`

**Producer:** Flink Job 4 `event-geo-correlator`
([`flink/jobs/event-geo-correlator`](../../flink/jobs/event-geo-correlator/)).
**Consumers:** Flink Job 5 `risk-feature-builder` (Phase 4) → `risk.features.v1`.
**Related:** [ADR-002](../adr/0002-flink-sql-vs-datastream.md),
[ADR-007](../adr/0007-idempotency-strategy.md),
[ADR-009](../adr/0009-cdc-current-state-design.md), PLAN §11.4 / §18 Job 4.

## Flow

```
events.deduplicated.v1 ─────────────┐
                                     ├─▶ event-geo-correlator ─▶ risk.impact_candidates.v1
ops.facilities.current.v1 (broadcast)┘        Haversine + magnitude threshold
```

Facility current-state (Job 3, JSON, compacted) is broadcast to every parallel instance; each event
is scanned against it. Deletes arrive as compacted tombstones (null value) and remove the facility
from broadcast state.

## Topic

| Topic | Key | cleanup.policy | Retention |
|---|---|---|---|
| `risk.impact_candidates.v1` | `impact_id` (= `source_event_id::asset_id`) | delete | PLAN §10.4 |

## Semantics (PLAN §11.4)

- **Threshold radius** by event type / severity
  ([`ImpactThreshold`](../../flink/jobs/event-geo-correlator/src/main/java/com/sentinelchain/flink/geo/ImpactThreshold.java)):

  | Event type | Condition | Radius |
  |---|---|---:|
  | earthquake | magnitude ≥ 7 | 250 km |
  | earthquake | magnitude ≥ 6 | 120 km |
  | earthquake | magnitude ≥ 5 | 50 km |
  | earthquake | magnitude < 5 or null | *no candidate* |

  Wildfire/flood radii are stubbed for when those sources land; the operator stays source-agnostic.

- **Distance** is the Haversine great-circle km event → facility
  ([`Haversine`](../../flink/common/src/main/java/com/sentinelchain/flink/common/geo/Haversine.java)).
  A candidate is emitted per facility with `distance_km <= radius_km`.
- **`geospatial_score`** = `max(0, 1 - distance_km/radius_km)` — 1 at the epicentre, 0 at the edge.
- **`impact_id`** = `source_event_id::asset_id` — deterministic, so a replay yields the same id and
  downstream stays idempotent (ADR-007). It is also the Kafka key.
- **`supplier_id`** is carried from facility current-state to save a join in Job 5.
- **Scope:** assets are **facilities** only (warehouses/routes later). Correlation is purely
  geometric — facility lifecycle status is carried but not filtered here; business filtering happens
  in the risk/alert stages.
- **Broadcast seeding:** facilities read from the earliest offset of the compacted topic, so in the
  demo (seed facilities, then inject the quake) they are present before the event. A genuinely-late
  facility update only affects later events — the known trade-off of a broadcast join.

## Value fields

See [`schemas/avro/risk.impact_candidates.v1.avsc`](../../schemas/avro/risk.impact_candidates.v1.avsc):
`impact_id`, `event_id`, `source`, `source_event_id`, `event_type`, `event_time`, `asset_type`,
`asset_id`, `supplier_id`, event/asset lat-lon, `distance_km`, `radius_km`, `inside_affected_area`,
`severity`, `geospatial_score`, `trace_id`, `tenant_id`, `calculated_at`.

## Metrics (PLAN §26)

Operator counters: `geo_events_correlated`, `geo_impacts_emitted`.

## Serialization

**Avro** via Confluent Schema Registry (ADR-001). Subject `risk.impact_candidates.v1-value`,
`BACKWARD`, key `impact_id` as plain UTF-8. The facility side is plain JSON (Job 3's `upsert-kafka`),
decoded by
[`FacilityUpdateDeserializer`](../../flink/jobs/event-geo-correlator/src/main/java/com/sentinelchain/flink/geo/FacilityUpdateDeserializer.java).

## Verifying (the PLAN §35 demo shape)

```bash
make up-full && make create-topics && make register-schemas
make register-connectors && make seed      # facility near Tokyo in ops.facilities.current.v1
make build-flink
make submit-job3 && make submit-job1 && make submit-job2 && make submit-job4
make run-usgs                              # a live M≥5 quake near a facility → an impact candidate

docker run --rm --network sentinelchain_default confluentinc/cp-schema-registry:7.6.1 \
  kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --topic risk.impact_candidates.v1 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property print.key=true --from-beginning --timeout-ms 8000
```
