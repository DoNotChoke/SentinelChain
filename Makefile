# SentinelChain developer entrypoints (PLAN §24).
# Override the Python launcher on Windows, e.g.:  make test PYTHON="py -3"
PYTHON ?= python
COMPOSE ?= docker compose

.DEFAULT_GOAL := help
.PHONY: help up up-full down reset logs ps bootstrap create-topics register-schemas \
        register-connectors migrate seed submit-job3 build-flink submit-job1 submit-job2 \
        submit-job4 run-usgs demo install lint format typecheck test test-integration

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ── Infrastructure ──────────────────────────────────────────────────────────
up: ## Start core infra (Kafka, Schema Registry, Postgres, OpenSearch, Redis)
	$(COMPOSE) --profile core up -d

up-full: ## Start the full stack (adds Connect, Flink, MinIO, MLflow, observability)
	$(COMPOSE) --profile full up -d

down: ## Stop all containers
	$(COMPOSE) --profile full down

reset: ## Stop and DELETE all volumes (wipes data)
	$(COMPOSE) --profile full down -v

logs: ## Tail logs (use S=<service> to filter)
	$(COMPOSE) logs -f $(S)

ps: ## List running containers
	$(COMPOSE) ps

## ── Bootstrap ───────────────────────────────────────────────────────────────
bootstrap: create-topics register-schemas register-connectors ## Topics + schemas + CDC connector
	@echo "bootstrap complete"

create-topics: ## Create Kafka topics
	bash scripts/create-topics.sh

register-schemas: ## Register Avro schemas with Schema Registry
	bash scripts/register-schemas.sh

register-connectors: ## Register Debezium Postgres CDC connector
	bash scripts/register-connectors.sh

migrate: ## Apply the operational DB schema (Alembic)
	$(PYTHON) -m alembic -c services/supply-chain-simulator/alembic.ini upgrade head

seed: ## Apply schema + seed the deterministic demo scenario into Postgres
	bash scripts/seed-operational-data.sh

submit-job3: ## Submit Flink Job 3 (operational-current-state) — requires up-full
	bash scripts/submit-flink-sql.sh operational_current_state.sql

## ── Flink DataStream jobs (Java) ────────────────────────────────────────────
# Build in a Maven container so the only host requirement stays Docker. The repo root is
# mounted (not just flink/) because the job bundles schemas/avro as classpath resources.
build-flink: ## Build the Java Flink job fat-jars (Maven in Docker)
	docker run --rm -v "$(CURDIR):/build" -v "$(HOME)/.m2:/root/.m2" -w /build/flink \
		maven:3.9-eclipse-temurin-17 mvn -q -B package

submit-job1: ## Submit Flink Job 1 (external-event-normalizer) — requires up-full + build-flink
	bash scripts/submit-flink-jar.sh

submit-job2: ## Submit Flink Job 2 (event-deduplicator) — requires up-full + build-flink
	bash scripts/submit-flink-jar.sh /opt/jobs/event-deduplicator/target/event-deduplicator.jar

submit-job4: ## Submit Flink Job 4 (event-geo-correlator) — requires up-full + build-flink
	bash scripts/submit-flink-jar.sh /opt/jobs/event-geo-correlator/target/event-geo-correlator.jar

run-usgs: ## Run the USGS ingestion service (poll → ext.usgs.raw.v1) — requires `make up`
	$(PYTHON) -m ingestion_usgs.main

demo: ## Run the end-to-end demo scenario (PLAN §35)
	bash scripts/run-demo-scenario.sh

## ── Python workflow ─────────────────────────────────────────────────────────
install: ## Install libs/common (editable) with dev extras
	$(PYTHON) -m pip install -e "libs/common[dev]"

lint: ## Lint with ruff
	$(PYTHON) -m ruff check .

format: ## Auto-format with ruff
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

typecheck: ## Type-check with mypy
	$(PYTHON) -m mypy libs/common/src

test: ## Run unit tests
	$(PYTHON) -m pytest

test-integration: ## Run integration tests (requires `make up`)
	$(PYTHON) -m pytest tests/integration
