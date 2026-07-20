# Flink Job 1 — `external-event-normalizer`

Reads `ext.usgs.raw.v1` (Avro/Confluent), applies the PLAN §28 data-quality rules, and routes each
record to either `events.normalized.v1` (canonical shape) or `audit.data_quality.v1`. First
DataStream/Java job (ADR-002); stateless map + side output, event-time watermarks on `event_time`.

See the [data contract](../../../docs/data-contracts/events-normalized.md) for the output schema
and semantics.

## Layout

| File | Role |
|---|---|
| [`NormalizerJob.java`](src/main/java/com/sentinelchain/flink/normalizer/NormalizerJob.java) | Wiring: Kafka source → watermarks → process → two Kafka sinks |
| [`NormalizeFunction.java`](src/main/java/com/sentinelchain/flink/normalizer/NormalizeFunction.java) | `ProcessFunction` splitting valid → main output, invalid → audit side output |
| [`UsgsNormalization.java`](src/main/java/com/sentinelchain/flink/normalizer/UsgsNormalization.java) | Pure normalization + validation logic (Flink-free, unit-tested) |

## Config

Overridable via `flink run` args or env (see `common/JobConfig`), nothing hard-coded (PLAN §36.4):

| Arg | Env | Default |
|---|---|---|
| `--bootstrap-servers` | `KAFKA_BOOTSTRAP_INTERNAL` | `kafka:29092` |
| `--schema-registry-url` | `SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` |
| `--consumer-group-id` | `CONSUMER_GROUP_ID` | `flink-external-event-normalizer` |

## Build, test, run

```bash
make build-flink      # mvn package in a Maven container → target/external-event-normalizer.jar
make up-full          # Flink session cluster + Kafka + Schema Registry
make create-topics register-schemas
make submit-job1      # flink run -d /opt/jobs/external-event-normalizer/target/…jar
```

Unit tests run inside `make build-flink` (`mvn package`). The delivery guarantee is at-least-once;
deduplication is Job 2's responsibility.
