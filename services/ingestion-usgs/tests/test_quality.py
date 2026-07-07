"""Unit tests for USGS data-quality checks (PLAN §28)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from ingestion_usgs.parser import UsgsEvent
from ingestion_usgs.quality import check_event

_NOW = datetime(2026, 7, 1, 12, 0, tzinfo=UTC)
_TOL = timedelta(seconds=300)


def _event(**overrides: object) -> UsgsEvent:
    base = {
        "source_event_id": "us7000valid",
        "magnitude": 6.2,
        "latitude": 35.1,
        "longitude": 139.2,
        "depth_km": 38.0,
        "place": "somewhere",
        "event_time": datetime(2026, 7, 1, 9, 0, tzinfo=UTC),
        "updated_time": datetime(2026, 7, 1, 9, 8, tzinfo=UTC),
        "source_url": "https://example.test",
    }
    base.update(overrides)
    return UsgsEvent(**base)  # type: ignore[arg-type]


def test_valid_event_has_no_reasons() -> None:
    assert check_event(_event(), now=_NOW, future_tolerance=_TOL) == []


def test_negative_magnitude_micro_quake_is_valid() -> None:
    assert check_event(_event(magnitude=-0.4), now=_NOW, future_tolerance=_TOL) == []


def test_out_of_range_latitude_flagged() -> None:
    reasons = check_event(_event(latitude=999.0), now=_NOW, future_tolerance=_TOL)
    assert any("latitude" in r for r in reasons)


def test_out_of_range_longitude_flagged() -> None:
    reasons = check_event(_event(longitude=-500.0), now=_NOW, future_tolerance=_TOL)
    assert any("longitude" in r for r in reasons)


def test_missing_magnitude_flagged() -> None:
    reasons = check_event(_event(magnitude=None), now=_NOW, future_tolerance=_TOL)
    assert any("magnitude" in r for r in reasons)


def test_absurd_magnitude_flagged() -> None:
    reasons = check_event(_event(magnitude=42.0), now=_NOW, future_tolerance=_TOL)
    assert any("magnitude" in r for r in reasons)


def test_future_event_flagged() -> None:
    future = _NOW + timedelta(hours=1)
    reasons = check_event(_event(event_time=future), now=_NOW, future_tolerance=_TOL)
    assert any("future" in r for r in reasons)


def test_event_within_tolerance_not_flagged() -> None:
    near = _NOW + timedelta(seconds=120)
    assert check_event(_event(event_time=near), now=_NOW, future_tolerance=_TOL) == []
