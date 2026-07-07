"""Unit tests for the in-memory cursor store and marker composition."""

from __future__ import annotations

import pytest
from ingestion_usgs.cursor import InMemoryCursorStore, marker_for


def test_marker_combines_version_and_hash() -> None:
    assert marker_for("2026-07-01T09:08:00Z", "abc") == "2026-07-01T09:08:00Z:abc"


@pytest.mark.asyncio
async def test_record_and_read_marker() -> None:
    store = InMemoryCursorStore()
    assert await store.seen_marker("us1") is None
    await store.record("us1", "m1")
    assert await store.seen_marker("us1") == "m1"
    await store.record("us1", "m2")
    assert await store.seen_marker("us1") == "m2"


@pytest.mark.asyncio
async def test_cursor_ms_roundtrip() -> None:
    store = InMemoryCursorStore()
    assert await store.get_cursor_ms() is None
    await store.set_cursor_ms(1751360880000)
    assert await store.get_cursor_ms() == 1751360880000
