# SentinelChain

> **Real-Time Supply Chain Risk Intelligence & Hybrid RAG Copilot**

SentinelChain correlates live global events (earthquakes, disasters, news) with internal
supply-chain state (suppliers, facilities, shipments, inventory) in near real time, raises
explainable risk alerts, and answers analyst questions through a grounded, validated hybrid
RAG copilot.

It is built as two pipelines on a shared Kafka backbone:

- **Risk pipeline (deterministic):** live events + Debezium CDC → Kafka → Flink stateful jobs
  → geospatial correlation → risk scoring → alerts.
- **Hybrid RAG pipeline:** documents + operational state + risk output → chunking/embedding →
  OpenSearch hybrid retrieval + structured tools → LLM → validation → grounded answer.

> Core principle: the LLM only synthesizes and explains — it never replaces business logic.
> All numbers come from structured sources; every important claim carries evidence and passes
> validation.

## Documentation

- [`PLAN.md`](PLAN.md) — full product/architecture specification.
- [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md) — phased implementation plan,
  decisions, and annotated repository structure.
- [`docs/adr/`](docs/adr/) — Architecture Decision Records.

## Status

Milestone 2 — **External event ingestion (USGS)** (done). The
[`ingestion-usgs`](services/ingestion-usgs/) service polls the USGS GeoJSON feed with a
persisted Redis cursor and an idempotent producer into `ext.usgs.raw.v1`; invalid records go to
`audit.data_quality.v1` (see the [data contract](docs/data-contracts/ext-usgs-raw.md)). A service
restart does not re-emit unchanged events. Events are serialized as **Avro** against the Schema
Registry ([`schemas/avro/`](schemas/avro/) is the source of truth, subjects pinned to `BACKWARD`);
producers do not auto-register, so an unreviewed schema fails fast (ADR-001). Compatibility is
enforced by [`tests/contract/`](tests/contract/).

Milestone 1 — **Operational data + CDC** (done). Postgres schema, the
[`supply-chain-simulator`](services/supply-chain-simulator/), Debezium CDC, and Flink Job 3
(`operational-current-state`) run end-to-end: a change in Postgres flows through `ops.public.*`
(Debezium) to compacted `ops.<entity>.current.v1` topics (see the
[data contract](docs/data-contracts/ops-current-state.md)). See the roadmap in
[`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md#e-lộ-trình-triển-khai-theo-giai-đoạn).

```bash
make up-full          # core + Flink/Connect
make seed             # apply schema + seed the demo scenario
make register-connectors   # Debezium Postgres source
make submit-job3      # Flink Job 3: operational-current-state

# Milestone 2 — USGS ingestion
make create-topics    # ensure ext.usgs.raw.v1 + audit.data_quality.v1 exist
make register-schemas # register the Avro schemas (required before the first produce)
make run-usgs         # poll USGS → ext.usgs.raw.v1
```

## Quick start

Requires Docker, GNU Make, and Python 3.12.

```bash
cp .env.example .env

# Start core infrastructure (single-broker Kafka, Schema Registry, Postgres, OpenSearch, Redis)
make up

# Stop / wipe
make down
make reset

# Python dev workflow
make lint
make format
make test
```

The local stack uses Docker Compose **profiles**:

- `core` — minimal stack for day-to-day development (default for `make up`).
- `full` — adds Flink, Kafka Connect, MinIO, MLflow, Prometheus, Grafana, OTel collector.

```bash
make up-full   # bring up the full stack
```

## Repository layout

See the annotated tree in
[`docs/IMPLEMENTATION_PLAN.md` §G](docs/IMPLEMENTATION_PLAN.md#g-cấu-trúc-thư-mục-dự-án-có-chú-giải).
