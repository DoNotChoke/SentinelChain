#!/usr/bin/env bash
# Apply the operational schema (Alembic) and seed the deterministic demo scenario (PLAN §35).
# Idempotent: migrations are versioned; the seed uses merge-on-primary-key.
set -euo pipefail

cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python}"
export POSTGRES_DSN="${POSTGRES_DSN:-postgresql://sentinel:sentinel@localhost:5432/sentinel}"

echo "==> Applying operational schema (alembic upgrade head)"
${PYTHON} -m alembic -c services/supply-chain-simulator/alembic.ini upgrade head

echo "==> Seeding demo scenario (seed-only, no continuous loop)"
SIMULATOR_SEED_ON_START=true SIMULATOR_RUN_LOOP=false \
  ${PYTHON} -m supply_chain_simulator.main

echo "==> Done."
