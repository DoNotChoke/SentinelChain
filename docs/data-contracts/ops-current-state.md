# Data contract — Operational current-state topics

**Producer:** Flink Job 3 `operational-current-state` ([`flink/sql/operational_current_state.sql`](../../flink/sql/operational_current_state.sql))
**Consumers:** risk pipeline jobs (Job 4 geo-correlator, Job 5 feature-builder — Phase 3+), read-model builders.
**Related:** [ADR-009](../adr/0009-cdc-current-state-design.md), [ADR-001](../adr/0001-kafka-serialization-format.md), PLAN §10.2/§11.5.

## Flow

```
simulator → Postgres → Debezium (ops.public.*, debezium-json) → Flink Job 3 (upsert-kafka) → ops.<entity>.current.v1
```

## Topics

| Topic | Key | cleanup.policy | Source table |
|---|---|---|---|
| `ops.suppliers.current.v1` | `supplier_id` | compact | `suppliers` |
| `ops.facilities.current.v1` | `facility_id` | compact | `facilities` |
| `ops.shipments.current.v1` | `shipment_id` | compact | `shipments` |
| `ops.inventory.current.v1` | `warehouse_id` + `product_id` | compact | `inventory` |
| `ops.purchase_orders.current.v1` | `purchase_order_id` | compact | `purchase_orders` |

## Semantics

- **Compacted, last-write-wins per key.** The latest record for a key is the entity's current
  state. Consumers rebuild current state by replaying the compacted log — replay-safe, no state
  is lost on restart (PLAN §30).
- **Deletes are tombstones** (null value). Log compaction and consumers drop the entity.
- Composite-key tables (`inventory`) use a composite JSON key `{warehouse_id, product_id}`.

## Serialization (Phase 1)

- **Key:** JSON (`{"<pk>": "..."}`).
- **Value:** JSON. See the ADR-001 CDC exception — JSON is used for CDC/current-state in Phase 1;
  Avro migration is deferred.

## Type mapping (from Debezium JSON)

Debezium is configured with `decimal.handling.mode=double`, `time.precision.mode=connect`,
`value.converter.schemas.enable=false`:

| Postgres type | current-state field type | Notes |
|---|---|---|
| `uuid`, `text`, `char` | `string` | |
| `numeric` | `double` | via `decimal.handling.mode=double` |
| `integer` | `int` | |
| `timestamptz` | `string` | ISO-8601 UTC, e.g. `2026-07-01T17:32:44.680267Z` (already normalized) |
| `date` | `int` | days since epoch (e.g. `20649`); downstream converts |

## Verifying

```bash
make up-full && make seed && make register-connectors && make submit-job3

# Materialize current state (last value per key) for suppliers:
docker exec sentinelchain-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ops.suppliers.current.v1 \
  --from-beginning --timeout-ms 8000 --property print.key=true
```

An UPDATE in Postgres appears as a new value on the topic; a DELETE appears as a tombstone.
