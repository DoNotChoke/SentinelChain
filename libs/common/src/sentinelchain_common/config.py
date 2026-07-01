"""Configuration loading via pydantic-settings.

Every service derives its settings from :class:`BaseServiceSettings`, which provides the
infrastructure connection details common to all of them. Values come from environment
variables (12-factor); see ``.env.example``. No secrets are hard-coded (PLAN §36.4).
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    """Base settings shared by all SentinelChain services."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # Deployment
    sentinel_env: str = "dev"
    service_name: str = "sentinelchain-service"
    log_level: str = "INFO"
    default_tenant_id: str = "demo"

    # Kafka / Schema Registry
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    kafka_topic_replication_factor: int = 1
    kafka_topic_min_insync_replicas: int = 1

    # Stores
    postgres_dsn: str = "postgresql://sentinel:sentinel@localhost:5432/sentinel"
    opensearch_url: str = "http://localhost:9200"
    redis_url: str = "redis://localhost:6379/0"

    # Observability
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    metrics_port: int = Field(default=9100, description="Prometheus scrape port")
