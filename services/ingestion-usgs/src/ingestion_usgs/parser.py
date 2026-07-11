"""Parse the USGS earthquake GeoJSON feed into normalized event records.

The USGS feed is a GeoJSON ``FeatureCollection``. Each ``Feature`` looks like::

    {
      "id": "us7000xxxx",
      "properties": {"mag": 6.2, "place": "...", "time": 1751360400000,
                     "updated": 1751360880000, "url": "..."},
      "geometry": {"type": "Point", "coordinates": [longitude, latitude, depth_km]}
    }

``time``/``updated`` are epoch **milliseconds**. ``coordinates`` is ``[lon, lat, depth]``.

This module only handles *structural* parsing (shape + type coercion). Semantic validation
(coordinate ranges, magnitude sanity, future event time) lives in :mod:`ingestion_usgs.quality`.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict

from sentinelchain_common import isoformat_utc


class FeatureParseError(ValueError):
    """Raised when a GeoJSON feature is structurally invalid (missing/typed-wrong fields)."""

    def __init__(self, message: str, *, source_event_id: str | None = None) -> None:
        super().__init__(message)
        self.source_event_id = source_event_id


class UsgsEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_event_id: str
    magnitude: float | None
    latitude: float
    longitude: float
    depth_km: float | None
    place: str | None
    event_time: datetime
    updated_time: datetime
    source_url: str | None

    @property
    def source_version(self) -> str:
        return isoformat_utc(self.updated_time)

    def payload(self) -> dict[str, object]:
        """The envelope payload written to ``ext.usgs.raw.v1`` (times as ISO-8601 UTC)."""
        return {
            "source_event_id": self.source_event_id,
            "magnitude": self.magnitude,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "depth_km": self.depth_km,
            "place": self.place,
            "event_time": isoformat_utc(self.event_time),
            "updated_time": isoformat_utc(self.updated_time),
            "source_url": self.source_url,
        }

    def payload_hash(self) -> str:
        """Stable hash of the payload, used with ``source_version`` as the dedup marker."""
        encoded = json.dumps(self.payload(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _epoch_ms_to_utc(value: object, field: str, source_event_id: str | None) -> datetime:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise FeatureParseError(
            f"'{field}' must be epoch milliseconds, got {value!r}", source_event_id=source_event_id
        )
    return datetime.fromtimestamp(value / 1000.0, tz=UTC)


def _as_float(value: object, field: str, source_event_id: str | None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise FeatureParseError(
            f"'{field}' must be a number, got {value!r}", source_event_id=source_event_id
        )
    return float(value)


def _opt_float(value: object, field: str, source_event_id: str | None) -> float | None:
    if value is None:
        return None
    return _as_float(value, field, source_event_id)


def parse_feature(feature: dict[str, object]) -> UsgsEvent:
    """Parse a single GeoJSON feature into a :class:`UsgsEvent`.

    Raises :class:`FeatureParseError` if the feature is structurally invalid.
    """
    raw_id = feature.get("id")
    source_event_id = raw_id if isinstance(raw_id, str) else None
    if not source_event_id:
        raise FeatureParseError("feature has no string 'id'", source_event_id=None)

    props = feature.get("properties")
    if not isinstance(props, dict):
        raise FeatureParseError(
            "'properties' missing or not an object", source_event_id=source_event_id
        )

    geometry = feature.get("geometry")
    if not isinstance(geometry, dict):
        raise FeatureParseError(
            "'geometry' missing or not an object", source_event_id=source_event_id
        )
    coords = geometry.get("coordinates")
    if not isinstance(coords, (list, tuple)) or len(coords) < 2:
        raise FeatureParseError(
            "'geometry.coordinates' must be [lon, lat, depth]", source_event_id=source_event_id
        )

    longitude = _as_float(coords[0], "longitude", source_event_id)
    latitude = _as_float(coords[1], "latitude", source_event_id)
    depth_km = _opt_float(coords[2], "depth_km", source_event_id) if len(coords) > 2 else None

    place = props.get("place")
    source_url = props.get("url")

    return UsgsEvent(
        source_event_id=source_event_id,
        magnitude=_opt_float(props.get("mag"), "magnitude", source_event_id),
        latitude=latitude,
        longitude=longitude,
        depth_km=depth_km,
        place=place if isinstance(place, str) else None,
        event_time=_epoch_ms_to_utc(props.get("time"), "time", source_event_id),
        updated_time=_epoch_ms_to_utc(props.get("updated"), "updated", source_event_id),
        source_url=source_url if isinstance(source_url, str) else None,
    )


def parse_feed(payload: dict[str, object]) -> tuple[list[UsgsEvent], list[FeatureParseError]]:
    """Parse a GeoJSON FeatureCollection.

    Returns ``(events, errors)``: structurally-valid events, and per-feature parse errors so
    the caller can route them to ``audit.data_quality.v1`` without dropping the whole batch.
    """
    features = payload.get("features")
    if not isinstance(features, list):
        raise FeatureParseError("payload has no 'features' array")

    events: list[UsgsEvent] = []
    errors: list[FeatureParseError] = []
    for feature in features:
        if not isinstance(feature, dict):
            errors.append(FeatureParseError(f"feature is not an object: {feature!r}"))
            continue
        try:
            events.append(parse_feature(feature))
        except FeatureParseError as exc:
            errors.append(exc)
    return events, errors
