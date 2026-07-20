#!/usr/bin/env bash
# Submit a shaded Flink DataStream job jar to the running session cluster via `flink run`.
# Job jars are mounted read-only into the jobmanager at /opt/jobs (see docker-compose.yml);
# build them first with `make build-flink`.
#
# Usage: scripts/submit-flink-jar.sh [container_jar_path] [-- extra flink run args...]
#   default jar: /opt/jobs/external-event-normalizer/target/external-event-normalizer.jar (Job 1)
set -euo pipefail

cd "$(dirname "$0")/.."

# Prevent Git Bash/MSYS from rewriting the absolute container path. No-op on Linux/macOS.
export MSYS_NO_PATHCONV=1

COMPOSE="${COMPOSE:-docker compose}"
JAR_PATH="${1:-/opt/jobs/external-event-normalizer/target/external-event-normalizer.jar}"
shift || true

echo "==> Submitting ${JAR_PATH} to Flink"
${COMPOSE} exec -T flink-jobmanager flink run -d "${JAR_PATH}" "$@"
echo "==> Submitted. Job UI: http://localhost:8082"
