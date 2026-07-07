# ingestion-usgs

Polls the **USGS earthquake GeoJSON feed** and produces canonical event envelopes into
`ext.usgs.raw.v1`. This is the first external-source ingestion service (PLAN §7.1, §11.1,
Milestone 2). Downstream, Flink Job 1 will normalize `ext.usgs.raw.v1` →
`events.normalized.v1` (Phase 3).

See [`docs/IMPLEMENTATION_PLAN.md` §Phase 2](../../docs/IMPLEMENTATION_PLAN.md) and the data
contract [`docs/data-contracts/ext-usgs-raw.md`](../../docs/data-contracts/ext-usgs-raw.md).

## What it does

Each poll cycle (`fetch → parse → validate → dedup → produce → commit cursor`):

1. **Fetch** the feed over HTTP with a timeout, exponential-backoff retry, and a circuit
   breaker (`libs/common` resilience primitives).
2. **Parse** the GeoJSON `FeatureCollection` into normalized events (epoch-ms → UTC ISO).
3. **Validate** data quality (PLAN §28): lat/lon ranges, magnitude sanity, non-future event
   time, non-empty id. Invalid records → `audit.data_quality.v1` (never emitted to the raw topic).
4. **Dedup** against a persisted cursor (Redis): an event is emitted only when new or when its
   `source_version` (upstream `updated` time) / payload hash changed. This makes a **restart not
   re-emit unchanged events**.
5. **Produce** with an idempotent producer, keyed by `source_event_id`.
6. **Commit the cursor only after the broker acknowledges** the produce (PLAN §11.1) — a crash
   between produce and commit leads to a safe re-emit, not a lost event.

## Serialization

M2a emits **JSON** envelopes via the shared `EnvelopeProducer`. Avro + Schema Registry is
wired in M2b (see [ADR-001](../../docs/adr/0001-kafka-serialization-format.md)).

## Run

```bash
make up                 # core infra (Kafka, Redis, ...)
make create-topics      # ensure ext.usgs.raw.v1 + audit.data_quality.v1 exist
python -m ingestion_usgs.main   # or: make run-usgs
```

## Config

All settings come from the environment (prefix-free, see `IngestionUsgsSettings`):

| Env | Default | Meaning |
|---|---|---|
| `USGS_FEED_URL` | `.../summary/all_hour.geojson` | USGS GeoJSON feed window |
| `USGS_POLL_INTERVAL_SECONDS` | `60` | Seconds between polls |
| `USGS_REQUEST_TIMEOUT_SECONDS` | `15` | Per-request HTTP timeout |
| `USGS_MAX_RETRIES` | `4` | Retry attempts per poll |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka |
| `REDIS_URL` | `redis://localhost:6379/0` | Cursor store |
| `USGS_RAW_TOPIC` | `ext.usgs.raw.v1` | Output topic |
| `DATA_QUALITY_TOPIC` | `audit.data_quality.v1` | Invalid records |
| `FUTURE_EVENT_TOLERANCE_SECONDS` | `300` | Max allowed event_time skew into the future |
| `CURSOR_TTL_SECONDS` | `2592000` | Dedup marker expiry (matches 30-day topic retention) |
| `USGS_HEALTH_PORT` | `8080` | `/health/live` + `/health/ready` |
| `METRICS_PORT` | `9100` | Prometheus `/metrics` |

## Endpoints

- `GET /health/live` — process is up.
- `GET /health/ready` — last poll succeeded (Redis reachable + feed processed).
- `GET /metrics` (port `METRICS_PORT`) — Prometheus metrics (PLAN §26):
  `source_poll_total`, `source_poll_failure_total`, `source_records_fetched_total`,
  `source_records_produced_total`, `source_records_duplicate_total`,
  `source_records_invalid_total`, `source_poll_latency_seconds`, `source_cursor_lag_seconds`.
