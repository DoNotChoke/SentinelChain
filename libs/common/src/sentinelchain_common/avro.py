"""Avro serialization wired to the Confluent Schema Registry (ADR-001).

Schemas under ``schemas/avro/<topic>.avsc`` are the source of truth; they are registered by
``scripts/register-schemas.sh`` under subject ``<topic>-value`` with BACKWARD compatibility.

By default the serializer does **not** auto-register schemas: it looks the schema up in the
registry and fails if it is absent. That makes the registry the contract gate — a producer
cannot quietly introduce an unreviewed schema — at the cost of requiring ``make register-schemas``
before first produce. Set ``avro_auto_register_schemas=true`` to relax this in throwaway dev.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from .config import BaseServiceSettings
from .envelope import EventEnvelope
from .time import to_utc


class SchemaNotFoundError(FileNotFoundError):
    """Raised when a topic has no ``.avsc`` under the configured schema directory."""


def load_schema(topic: str, schema_dir: str | Path) -> str:
    """Read the Avro schema for ``topic`` (file convention: ``<topic>.avsc``)."""
    path = Path(schema_dir) / f"{topic}.avsc"
    if not path.is_file():
        raise SchemaNotFoundError(
            f"no Avro schema for topic '{topic}': expected {path}. "
            "Schemas live in schemas/avro (ADR-001)."
        )
    return path.read_text(encoding="utf-8")


def envelope_to_avro(envelope: EventEnvelope) -> dict[str, Any]:
    """Map an :class:`EventEnvelope` to its Avro record dict.

    Envelope timestamps stay as tz-aware ``datetime`` (Avro ``timestamp-millis`` — fastavro
    encodes them to epoch millis), which is what lets Flink read ``event_time`` directly as a
    watermarked TIMESTAMP. The ``payload`` dict passes through untouched, so the payload hash
    used for dedup keeps its meaning (ADR-007).
    """
    return {
        "event_id": envelope.event_id,
        "event_type": envelope.event_type,
        "event_version": envelope.event_version,
        "source": envelope.source,
        "source_event_id": envelope.source_event_id,
        "source_version": envelope.source_version,
        "event_time": to_utc(envelope.event_time),
        "ingested_at": to_utc(envelope.ingested_at),
        "trace_id": envelope.trace_id,
        "correlation_id": envelope.correlation_id,
        "tenant_id": envelope.tenant_id,
        "payload": dict(envelope.payload),
    }


def envelope_from_avro(record: Mapping[str, Any]) -> EventEnvelope:
    """Rebuild an :class:`EventEnvelope` from a decoded Avro record."""
    return EventEnvelope(
        event_id=record["event_id"],
        event_type=record["event_type"],
        event_version=record["event_version"],
        source=record["source"],
        source_event_id=record["source_event_id"],
        source_version=record.get("source_version"),
        event_time=record["event_time"],
        ingested_at=record["ingested_at"],
        trace_id=record["trace_id"],
        correlation_id=record.get("correlation_id"),
        tenant_id=record["tenant_id"],
        payload=dict(record["payload"]),
    )


def schema_registry_client(settings: BaseServiceSettings) -> SchemaRegistryClient:
    return SchemaRegistryClient({"url": settings.schema_registry_url})


class AvroEnvelopeSerializer:
    """Serialize an :class:`EventEnvelope` to Avro bytes for one topic.

    One instance per topic: the payload schema differs per topic, so the schema — and the
    registry subject — is bound at construction.
    """

    def __init__(
        self,
        settings: BaseServiceSettings,
        topic: str,
        *,
        client: SchemaRegistryClient | None = None,
    ) -> None:
        self._topic = topic
        self._serializer = AvroSerializer(
            client or schema_registry_client(settings),
            load_schema(topic, settings.avro_schema_dir),
            to_dict=lambda envelope, _ctx: envelope_to_avro(envelope),
            conf={
                "auto.register.schemas": settings.avro_auto_register_schemas,
                "normalize.schemas": True,
            },
        )

    @property
    def topic(self) -> str:
        return self._topic

    def __call__(self, envelope: EventEnvelope) -> bytes:
        data: bytes | None = self._serializer(
            envelope, SerializationContext(self._topic, MessageField.VALUE)
        )
        if data is None:  # pragma: no cover - only when the envelope itself is None
            raise ValueError("Avro serialization produced no bytes")
        return data


class AvroEnvelopeDeserializer:
    """Decode Avro bytes from one topic back into an :class:`EventEnvelope`."""

    def __init__(
        self,
        settings: BaseServiceSettings,
        topic: str,
        *,
        client: SchemaRegistryClient | None = None,
    ) -> None:
        self._topic = topic
        self._deserializer = AvroDeserializer(
            client or schema_registry_client(settings),
            load_schema(topic, settings.avro_schema_dir),
            from_dict=lambda record, _ctx: envelope_from_avro(record),
        )

    def __call__(self, data: bytes) -> EventEnvelope:
        envelope: EventEnvelope | None = self._deserializer(
            data, SerializationContext(self._topic, MessageField.VALUE)
        )
        if envelope is None:  # pragma: no cover - tombstone (null value) has no envelope
            raise ValueError("Avro deserialization produced no envelope (tombstone?)")
        return envelope
