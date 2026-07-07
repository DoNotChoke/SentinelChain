#!/usr/bin/env bash
# Submit a Flink SQL script to the running session cluster via the SQL client.
# The script is mounted read-only into the jobmanager at /opt/sql (see docker-compose.yml).
#
# Usage: scripts/submit-flink-sql.sh [sql_filename]
#   default: operational_current_state.sql (Job 3)
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE="${COMPOSE:-docker compose}"
SQL_FILE="${1:-operational_current_state.sql}"

echo "==> Submitting /opt/sql/${SQL_FILE} to Flink"
${COMPOSE} exec -T flink-jobmanager ./bin/sql-client.sh -f "/opt/sql/${SQL_FILE}"
echo "==> Submitted. Job UI: http://localhost:8082"
