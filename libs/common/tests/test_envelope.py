from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from sentinelchain_common import EventEnvelope


def _envelope(**overrides: object) -> EventEnvelope:
    base: dict[str, object] = {
        "event_type": "earthquake",
        "source": "usgs",
        "source_event_id": "us7000xxxx",
        "event_time": datetime(2026, 7, 1, 9, 10, tzinfo=UTC),
        "payload": {"magnitude": 6.2},
    }
    base.update(overrides)
    return EventEnvelope(**base)  # type: ignore[arg-type]


def test_defaults_are_populated() -> None:
    env = _envelope()
    assert env.event_id
    assert env.trace_id
    assert env.event_version == 1
    assert env.tenant_id == "demo"
    assert env.ingested_at.tzinfo is not None


def test_dedup_key_is_source_plus_source_event_id() -> None:
    env = _envelope()
    assert env.dedup_key() == "usgs:us7000xxxx"


def test_payload_hash_is_stable_and_order_independent() -> None:
    a = _envelope(payload={"magnitude": 6.2, "depth": 38})
    b = _envelope(payload={"depth": 38, "magnitude": 6.2})
    assert a.payload_hash() == b.payload_hash()


def test_payload_hash_changes_with_payload() -> None:
    a = _envelope(payload={"magnitude": 6.2})
    b = _envelope(payload={"magnitude": 6.3})
    assert a.payload_hash() != b.payload_hash()


def test_extra_fields_rejected() -> None:
    with pytest.raises(ValidationError):
        _envelope(unexpected="nope")
