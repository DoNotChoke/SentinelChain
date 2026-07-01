"""Time utilities. All datetimes in SentinelChain are timezone-aware UTC (PLAN §36.11)."""

from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    """Return the current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def to_utc(dt: datetime) -> datetime:
    """Coerce a datetime to UTC. Naive datetimes are rejected.

    We deliberately reject naive datetimes rather than assuming a timezone, to avoid
    silently producing wrong event times.
    """
    if dt.tzinfo is None:
        raise ValueError("naive datetime is not allowed; provide a timezone-aware value")
    return dt.astimezone(UTC)


def isoformat_utc(dt: datetime) -> str:
    """Serialize a datetime as an ISO-8601 string in UTC with a trailing 'Z'."""
    return to_utc(dt).isoformat().replace("+00:00", "Z")
