"""Cursor / dedup persistence for USGS ingestion"""

from __future__ import annotations

from typing import Protocol

from redis.asyncio import Redis

_EVENT_PREFIX = "usgs:event:"
_CURSOR_KEY = "usgs:cursor:last_updated_ms"


class CursorStore(Protocol):
    async def seen_marker(self, source_event_id: str) -> str | None: ...
    async def record(self, source_event_id: str, marker: str) -> None: ...
    async def get_cursor_ms(self) -> int | None: ...
    async def set_cursor_ms(self, value: int) -> None: ...
    async def ping(self) -> bool: ...
    async def aclose(self) -> None: ...


def marker_for(source_version: str, payload_hash: str) -> str:
    """Compose the dedup marker stored per ``source_event_id``."""
    return f"{source_version}:{payload_hash}"


class RedisCursorStore:
    def __init__(self, redis_url: str, *, ttl_seconds: int) -> None:
        self._redis: Redis = Redis.from_url(redis_url, decode_responses=True)
        self._ttl = ttl_seconds

    async def seen_marker(self, source_event_id: str) -> str | None:
        value: str | None = await self._redis.get(_EVENT_PREFIX + source_event_id)
        return value

    async def record(self, source_event_id: str, marker: str) -> None:
        await self._redis.set(_EVENT_PREFIX + source_event_id, marker, ex=self._ttl)

    async def get_cursor_ms(self) -> int | None:
        value: str | None = await self._redis.get(_CURSOR_KEY)
        return int(value) if value is not None else None

    async def set_cursor_ms(self, value: int) -> None:
        await self._redis.set(_CURSOR_KEY, value)

    async def ping(self) -> bool:
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    async def aclose(self) -> None:
        await self._redis.aclose()


class InMemoryCursorStore:
    """In-memory cursor store for unit tests (no Redis required)."""

    def __init__(self) -> None:
        self._markers: dict[str, str] = {}
        self._cursor_ms: int | None = None

    async def seen_marker(self, source_event_id: str) -> str | None:
        return self._markers.get(source_event_id)

    async def record(self, source_event_id: str, marker: str) -> None:
        self._markers[source_event_id] = marker

    async def get_cursor_ms(self) -> int | None:
        return self._cursor_ms

    async def set_cursor_ms(self, value: int) -> None:
        self._cursor_ms = value

    async def ping(self) -> bool:
        return True

    async def aclose(self) -> None:
        return None
