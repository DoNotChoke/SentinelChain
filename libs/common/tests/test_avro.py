"""Avro mapping tests (ADR-001).

These exercise the envelope ↔ Avro record mapping against the real ``schemas/avro/*.avsc``
using fastavro directly, so no Schema Registry is needed. Registry compatibility is covered by
``tests/contract`` (integration).

The mapping's sharp edge is time: envelope timestamps cross the wire as ``timestamp-millis``,
so a wrong unit or a dropped timezone would silently shift every event.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import fastavro
import pytest

from sentinelchain_common import EventEnvelope
from sentinelchain_common.avro import (
    SchemaNotFoundError,
    envelope_from_avro,
    envelope_to_avro,
    load_schema,
)

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "schemas" / "avro"
USGS_TOPIC = "ext.usgs.raw.v1"
DQ_TOPIC = "audit.data_quality.v1"


def _roundtrip(envelope: EventEnvelope, topic: str) -> EventEnvelope:
    """Encode with fastavro against the on-disk schema, then decode back."""
    schema = fastavro.parse_schema(json.loads(load_schema(topic, SCHEMA_DIR)))
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, schema, envelope_to_avro(envelope))
    buffer.seek(0)
    record: dict[str, Any] = fastavro.schemaless_reader(buffer, schema)  # type: ignore[assignment]
    return envelope_from_avro(record)


def _usgs_envelope(**overrides: Any) -> EventEnvelope:
    payload = {
        "source_event_id": "us7000valid",
        "magnitude": 6.2,
        "latitude": 35.1,
        "longitude": 139.2,
        "depth_km": 38.0,
        "place": "Near the east coast of Honshu, Japan",
        "event_time": "2026-07-01T09:00:00Z",
        "updated_time": "2026-07-01T09:08:00Z",
        "source_url": "https://earthquake.usgs.gov/earthquakes/eventpage/us7000valid",
    }
    defaults: dict[str, Any] = {
        "event_type": "usgs.earthquake.raw",
        "source": "usgs",
        "source_event_id": "us7000valid",
        "source_version": "2026-07-01T09:08:00Z",
        "event_time": datetime(2026, 7, 1, 9, 0, 0, tzinfo=UTC),
        "payload": payload,
    }
    return EventEnvelope(**(defaults | overrides))


def test_usgs_envelope_survives_roundtrip() -> None:
    original = _usgs_envelope()
    decoded = _roundtrip(original, USGS_TOPIC)

    assert decoded.event_id == original.event_id
    assert decoded.source_event_id == original.source_event_id
    assert decoded.source_version == original.source_version
    assert decoded.tenant_id == original.tenant_id
    assert decoded.payload == original.payload


def test_event_time_keeps_its_instant_and_utc_offset() -> None:
    """A non-UTC input must come back as the *same instant*, expressed in UTC."""
    tokyo = timezone(timedelta(hours=9))
    original = _usgs_envelope(event_time=datetime(2026, 7, 1, 18, 0, 0, tzinfo=tokyo))

    decoded = _roundtrip(original, USGS_TOPIC)

    assert decoded.event_time == datetime(2026, 7, 1, 9, 0, 0, tzinfo=UTC)
    assert decoded.event_time.tzinfo is not None


def test_millisecond_precision_is_preserved() -> None:
    original = _usgs_envelope(event_time=datetime(2026, 7, 1, 9, 0, 0, 123000, tzinfo=UTC))

    decoded = _roundtrip(original, USGS_TOPIC)

    assert decoded.event_time == datetime(2026, 7, 1, 9, 0, 0, 123000, tzinfo=UTC)


def test_naive_event_time_is_rejected_before_the_wire() -> None:
    """Naive datetimes must never reach Kafka (PLAN §36.11) — to_utc refuses them."""
    envelope = EventEnvelope.model_construct(
        event_type="usgs.earthquake.raw",
        source="usgs",
        source_event_id="us7000naive",
        event_time=datetime(2026, 7, 1, 9, 0, 0),  # deliberately naive
        ingested_at=datetime(2026, 7, 1, 9, 1, 0, tzinfo=UTC),
        payload={},
    )

    with pytest.raises(ValueError, match="naive datetime"):
        envelope_to_avro(envelope)


def test_nullable_payload_fields_roundtrip_as_none() -> None:
    """magnitude/depth_km/place/source_url are unions with null — a young event has no mag yet."""
    original = _usgs_envelope(
        payload={
            "source_event_id": "us7000partial",
            "magnitude": None,
            "latitude": 35.1,
            "longitude": 139.2,
            "depth_km": None,
            "place": None,
            "event_time": "2026-07-01T09:00:00Z",
            "updated_time": "2026-07-01T09:00:00Z",
            "source_url": None,
        }
    )

    decoded = _roundtrip(original, USGS_TOPIC)

    assert decoded.payload["magnitude"] is None
    assert decoded.payload["place"] is None
    assert decoded.payload["latitude"] == 35.1


def test_data_quality_envelope_roundtrip() -> None:
    original = EventEnvelope(
        event_type="data_quality.violation",
        source="ingestion-usgs",
        source_event_id="unknown",
        event_time=datetime(2026, 7, 1, 9, 0, 0, tzinfo=UTC),
        payload={
            "origin_source": "usgs",
            "source_event_id": None,
            "reasons": ["latitude 200.0 out of range [-90, 90]"],
            "raw": '{"id": "us7000bad"}',
        },
    )

    decoded = _roundtrip(original, DQ_TOPIC)

    assert decoded.payload["reasons"] == ["latitude 200.0 out of range [-90, 90]"]
    assert decoded.payload["source_event_id"] is None
    assert json.loads(decoded.payload["raw"]) == {"id": "us7000bad"}


def test_unknown_topic_has_no_schema() -> None:
    with pytest.raises(SchemaNotFoundError, match="no Avro schema for topic"):
        load_schema("ext.nonexistent.v1", SCHEMA_DIR)
