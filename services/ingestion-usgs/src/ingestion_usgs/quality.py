"""Data-quality checks for USGS events (PLAN §28).

Invalid records are not emitted to ``ext.usgs.raw.v1``; instead the caller routes a
data-quality audit record to ``audit.data_quality.v1`` (optionally a DLQ).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from .parser import UsgsEvent

# USGS magnitudes range from small negatives (micro-quakes) to ~9.5 historically.
MIN_MAGNITUDE = -2.0
MAX_MAGNITUDE = 12.0


def check_event(
    event: UsgsEvent,
    *,
    now: datetime,
    future_tolerance: timedelta,
) -> list[str]:
    """Return a list of failure reasons for ``event``; empty means the record is valid.

    Checks (PLAN §28 USGS):
      - latitude in [-90, 90]
      - longitude in [-180, 180]
      - magnitude present and within a plausible range
      - event_time not further in the future than ``future_tolerance``
      - source_event_id non-empty
    """
    reasons: list[str] = []

    if not event.source_event_id:
        reasons.append("source_event_id is empty")
    if not (-90.0 <= event.latitude <= 90.0):
        reasons.append(f"latitude {event.latitude} out of range [-90, 90]")
    if not (-180.0 <= event.longitude <= 180.0):
        reasons.append(f"longitude {event.longitude} out of range [-180, 180]")
    if event.magnitude is None:
        reasons.append("magnitude is missing")
    elif not (MIN_MAGNITUDE <= event.magnitude <= MAX_MAGNITUDE):
        reasons.append(
            f"magnitude {event.magnitude} out of range [{MIN_MAGNITUDE}, {MAX_MAGNITUDE}]"
        )
    if event.event_time > now + future_tolerance:
        reasons.append(f"event_time {event.event_time.isoformat()} is too far in the future")

    return reasons
