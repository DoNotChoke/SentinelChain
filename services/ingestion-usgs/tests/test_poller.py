"""Unit tests for the poll cycle, incl. the 'restart does not duplicate' acceptance (PLAN §37)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_usgs.config import IngestionUsgsSettings
from ingestion_usgs.cursor import InMemoryCursorStore
from ingestion_usgs.parser import UsgsEvent
from ingestion_usgs.poller import Poller

_FIXTURE = Path(__file__).parent / "fixtures" / "usgs_sample.json"


class FakeClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    async def fetch_feed(self) -> dict[str, object]:
        return self._payload


class FakeProducer:
    """Records produced events and simulates broker acks (all delivered by default)."""

    def __init__(self, *, deliver: bool = True) -> None:
        self.events: list[UsgsEvent] = []
        self.data_quality: list[tuple[str | None, list[str]]] = []
        self.deliver = deliver
        self._pending: list[str] = []

    def produce_event(self, event: UsgsEvent) -> None:
        self.events.append(event)
        self._pending.append(event.source_event_id)

    def produce_data_quality(
        self, source_event_id: str | None, reasons: list[str], raw: dict[str, object] | None
    ) -> None:
        self.data_quality.append((source_event_id, reasons))

    def flush_and_confirm(self, timeout: float = 10.0) -> tuple[set[str], int]:
        delivered = set(self._pending) if self.deliver else set()
        remaining = 0 if self.deliver else len(self._pending)
        self._pending = []
        return delivered, remaining


def _payload() -> dict[str, object]:
    return json.loads(_FIXTURE.read_text(encoding="utf-8"))


def _settings() -> IngestionUsgsSettings:
    # Freeze future-tolerance behaviour independent of wall clock: the fixture events are in
    # 2026-07-01, comfortably in the past relative to any real run.
    return IngestionUsgsSettings()


@pytest.mark.asyncio
async def test_first_poll_produces_valid_events_and_routes_invalid() -> None:
    producer = FakeProducer()
    cursor = InMemoryCursorStore()
    poller = Poller(_settings(), FakeClient(_payload()), producer, cursor)  # type: ignore[arg-type]

    stats = await poller.poll_once()

    produced_ids = {e.source_event_id for e in producer.events}
    # us7000valid + us7000micro are valid; us7000badlat (bad lat) + us7000nogeom (parse error).
    assert produced_ids == {"us7000valid", "us7000micro"}
    assert stats.produced == 2
    assert stats.invalid == 2  # one quality failure + one structural parse error
    dq_ids = {i for i, _ in producer.data_quality}
    assert "us7000badlat" in dq_ids
    assert "us7000nogeom" in dq_ids


@pytest.mark.asyncio
async def test_second_poll_of_same_feed_produces_nothing() -> None:
    """Re-polling (or restarting) with an unchanged feed must not re-emit events."""
    cursor = InMemoryCursorStore()
    settings = _settings()

    first = FakeProducer()
    await Poller(settings, FakeClient(_payload()), first, cursor).poll_once()  # type: ignore[arg-type]
    assert len(first.events) == 2

    second = FakeProducer()
    stats = await Poller(settings, FakeClient(_payload()), second, cursor).poll_once()  # type: ignore[arg-type]
    assert second.events == []
    assert stats.produced == 0
    assert stats.duplicates == 2


@pytest.mark.asyncio
async def test_undelivered_events_are_not_committed() -> None:
    """If the broker never acks, the cursor is not advanced, so the next poll retries."""
    cursor = InMemoryCursorStore()
    settings = _settings()

    failed = FakeProducer(deliver=False)
    stats = await Poller(settings, FakeClient(_payload()), failed, cursor).poll_once()  # type: ignore[arg-type]
    assert stats.produced == 0
    assert stats.undelivered == 2

    # Next poll with a working broker re-emits the same events (nothing was committed).
    ok = FakeProducer(deliver=True)
    stats2 = await Poller(settings, FakeClient(_payload()), ok, cursor).poll_once()  # type: ignore[arg-type]
    assert stats2.produced == 2
