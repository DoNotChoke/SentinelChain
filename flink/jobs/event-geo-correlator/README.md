# Flink Job 4 — `event-geo-correlator`

Joins `events.deduplicated.v1` against the facility current-state (`ops.facilities.current.v1`, held
in **broadcast state**) and emits one `risk.impact_candidates.v1` record per facility within the
event's impact radius (Haversine + magnitude thresholds, PLAN §11.4). Second stateful DataStream/Java
job (ADR-002) — the heart of the risk pipeline.

See the [data contract](../../../docs/data-contracts/risk-impact-candidates.md) for full semantics.

## Layout

| File | Role |
|---|---|
| [`GeoCorrelatorJob.java`](src/main/java/com/sentinelchain/flink/geo/GeoCorrelatorJob.java) | Wiring: events + broadcast(facilities) → process → sink |
| [`GeoCorrelationFunction.java`](src/main/java/com/sentinelchain/flink/geo/GeoCorrelationFunction.java) | `BroadcastProcessFunction`: facility state + Haversine scan |
| [`ImpactThreshold.java`](src/main/java/com/sentinelchain/flink/geo/ImpactThreshold.java) | Radius rules per event type / magnitude (unit-tested) |
| [`GeoScore.java`](src/main/java/com/sentinelchain/flink/geo/GeoScore.java) | Proximity score (unit-tested) |
| [`ImpactCandidates.java`](src/main/java/com/sentinelchain/flink/geo/ImpactCandidates.java) | Builds the impact Avro record (unit-tested) |
| [`FacilityUpdateDeserializer.java`](src/main/java/com/sentinelchain/flink/geo/FacilityUpdateDeserializer.java) | JSON current-state → `FacilityUpdate` (handles tombstones) |
| `Facility` / `FacilityUpdate` | POJOs for broadcast state |

`Haversine` lives in [`flink/common`](../../common/) (shared, unit-tested there).

## Config

| Arg | Env | Default |
|---|---|---|
| `--bootstrap-servers` | `KAFKA_BOOTSTRAP_INTERNAL` | `kafka:29092` |
| `--schema-registry-url` | `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |
| `--consumer-group-id` | `CONSUMER_GROUP_ID` | `flink-event-geo-correlator` (facilities use `<group>-facilities`) |

## Build, run

```bash
make build-flink
make up-full && make create-topics register-schemas register-connectors && make seed
make submit-job3 submit-job1 submit-job2 submit-job4
make run-usgs
```

Metrics on the operator (Flink UI): `geo_events_correlated`, `geo_impacts_emitted`.
