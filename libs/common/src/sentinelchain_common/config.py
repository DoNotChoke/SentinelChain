"""Configuration for SentinelChain"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    """Base settings for SentinelChain services."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    sentinel_env: str = "dev"
    service_name: str = "sentinelchain-service"
    log_level: str = "INFO"
    default_tenant_id: str = "demo"

    # Kafka / Schema Registry
    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    kafka_topic_replication_factor: int = 1
    kafka_topic_min_insync_replicas: int = 1

    # Avro (ADR-001). Schemas are the source of truth on disk and must be registered before a
    # producer can use them; auto-registration is off so the registry stays the contract gate.
    avro_schema_dir: str = "schemas/avro"
    avro_auto_register_schemas: bool = False

    # Storage
    postgres_dsn: str = "postgresql://sentinel:sentinel@localhost:5432/sentinel"
    opensearch_url: str = "http://localhost:9200"
    redis_url: str = "redis://localhost:6379/0"

    # Observability
    otel_exporter_otlp_endpoint: str = "http://localhost:4317"
    metrics_port: int = Field(default=9100, description="Prometheus scrape port")
