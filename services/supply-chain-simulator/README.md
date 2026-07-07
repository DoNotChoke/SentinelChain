# supply-chain-simulator

Simulates a company's operational system (ERP/WMS): it continuously writes and mutates
suppliers, facilities, products, warehouses, purchase orders, shipments and inventory in
**PostgreSQL**. Postgres is the CDC source — Debezium captures these row changes into Kafka
(`ops.public.*`), which Flink Job 3 turns into compacted current-state topics
(`ops.<entity>.current.v1`).

See [`docs/IMPLEMENTATION_PLAN.md` §Phase 1](../../docs/IMPLEMENTATION_PLAN.md) and PLAN §7.6 / §8.

## What it does

- **Deterministic seed** (`SIMULATOR_SEED_ON_START=true`): builds the demo scenario from
  PLAN §35 — a Japanese supplier, a facility near Tokyo, a destination warehouse, products, a
  purchase order, 5 active shipments, and inventory with ~3.1 days remaining. Uses stable UUIDs
  so `make demo` is reproducible.
- **Continuous simulation loop**: every tick it applies small changes (shipment status
  transitions, ETA drift, inventory consumption, occasional new PO, occasional facility pause).
  Each change bumps `updated_at`, which produces a CDC event.

## CDC tables

`REPLICA IDENTITY FULL` is set on the five tables that Debezium captures so `before` images are
complete on UPDATE/DELETE: `suppliers`, `facilities`, `shipments`, `inventory`,
`purchase_orders` (PLAN §10.2, ADR-009).

## Run

```bash
# 1. Apply the schema (Alembic)
alembic -c services/supply-chain-simulator/alembic.ini upgrade head

# 2. Seed only (deterministic demo data)
make seed

# 3. Run the continuous simulator (seed + loop)
python -m supply_chain_simulator.main
```

## Config

All settings come from the environment (prefix-free, see `SimulatorSettings`):

| Env | Default | Meaning |
|---|---|---|
| `POSTGRES_DSN` | `postgresql://sentinel:sentinel@localhost:5432/sentinel` | Operational DB |
| `SIMULATOR_TICK_INTERVAL_SECONDS` | `2.0` | Seconds between simulation ticks |
| `SIMULATOR_SEED_ON_START` | `true` | Seed the demo scenario on startup (idempotent) |
| `SIMULATOR_RANDOM_SEED` | _(unset)_ | Fix the RNG for reproducible runs |
| `SIMULATOR_RUN_LOOP` | `true` | Run the continuous loop (set false to seed-and-exit) |
| `SIMULATOR_HEALTH_PORT` | `8080` | `/health/live` + `/health/ready` |
| `METRICS_PORT` | `9100` | Prometheus `/metrics` |

## Endpoints

- `GET /health/live` — process is up.
- `GET /health/ready` — Postgres reachable.
- `GET /metrics` (port `METRICS_PORT`) — Prometheus metrics.
