"""Unit tests for the USGS GeoJSON parser."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from ingestion_usgs.parser import FeatureParseError, parse_feature, parse_feed

_FIXTURE = Path(__file__).parent / "fixtures" / "usgs_sample.json"


def _load() -> dict[str, object]:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def test_parse_feed_separates_valid_and_structurally_broken() -> None:
    events, errors = parse_feed(_load())
    # us7000valid, us7000micro, us7000badlat parse structurally; us7000nogeom has null geometry.
    ids = {e.source_event_id for e in events}
    assert ids == {"us7000valid", "us7000micro", "us7000badlat"}
    assert len(errors) == 1
    assert errors[0].source_event_id == "us7000nogeom"


def test_parse_feature_maps_fields_and_times() -> None:
    feature = next(f for f in _load()["features"] if f["id"] == "us7000valid")
    event = parse_feature(feature)

    assert event.source_event_id == "us7000valid"
    assert event.magnitude == 6.2
    assert event.latitude == 35.1
    assert event.longitude == 139.2
    assert event.depth_km == 38.0
    assert event.event_time == datetime(2026, 7, 1, 9, 0, tzinfo=UTC)
    assert event.updated_time == datetime(2026, 7, 1, 9, 8, tzinfo=UTC)
    # source_version is the ISO-8601 'updated' time and drives downstream dedup/versioning.
    assert event.source_version == "2026-07-01T09:08:00Z"


def test_payload_shape_matches_contract() -> None:
    feature = next(f for f in _load()["features"] if f["id"] == "us7000valid")
    payload = parse_feature(feature).payload()
    assert set(payload) == {
        "source_event_id",
        "magnitude",
        "latitude",
        "longitude",
        "depth_km",
        "place",
        "event_time",
        "updated_time",
        "source_url",
    }
    assert payload["event_time"] == "2026-07-01T09:00:00Z"


def test_missing_id_raises() -> None:
    try:
        parse_feature({"properties": {}, "geometry": {"coordinates": [0, 0]}})
    except FeatureParseError as exc:
        assert exc.source_event_id is None
    else:  # pragma: no cover - explicit failure
        raise AssertionError("expected FeatureParseError")


def test_non_numeric_time_raises() -> None:
    feature = {
        "id": "x",
        "properties": {"mag": 1.0, "time": "not-a-number", "updated": 1},
        "geometry": {"coordinates": [0.0, 0.0, 1.0]},
    }
    try:
        parse_feature(feature)
    except FeatureParseError as exc:
        assert exc.source_event_id == "x"
    else:  # pragma: no cover
        raise AssertionError("expected FeatureParseError")
