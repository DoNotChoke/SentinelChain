"""Core poll cycle: fetch → parse → validate → dedup → produce → commit cursor.

The ordering enforces the ingestion contract (PLAN §11.1): an event's dedup marker is recorded
**only after** the broker acknowledges its produce, so a crash between produce and commit leads
to a safe re-emit (idempotent producer + downstream dedup) rather than a lost event.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from sentinelchain_common import get_logger, utcnow

from .config import IngestionUsgsSettings
from .cursor import CursorStore, marker_for
from .metrics import (
    CURSOR_LAG,
    POLL_FAILURE_TOTAL,
    POLL_LATENCY,
    POLL_TOTAL,
    RECORDS_DUPLICATE_TOTAL,
    RECORDS_FETCHED_TOTAL,
    RECORDS_INVALID_TOTAL,
    RECORDS_PRODUCED_TOTAL,
    SOURCE,
)
from .parser import parse_feed
from .producer import RawEventProducer
from .quality import check_event

_log = get_logger("ingestion-usgs.poller")


@dataclass(slots=True)
class PollStats:
    fetched: int = 0
    produced: int = 0
    duplicates: int = 0
    invalid: int = 0
    undelivered: int = 0


class _FeedClient:
    async def fetch_feed(self) -> dict[str, object]: ...  # pragma: no cover - typing only


class Poller:
    def __init__(
        self,
        settings: IngestionUsgsSettings,
        client: _FeedClient,
        producer: RawEventProducer,
        cursor: CursorStore,
    ) -> None:
        self._settings = settings
        self._client = client
        self._producer = producer
        self._cursor = cursor
        self._future_tolerance = timedelta(seconds=settings.future_event_tolerance_seconds)

    async def poll_once(self) -> PollStats:
        POLL_TOTAL.labels(SOURCE).inc()
        with POLL_LATENCY.labels(SOURCE).time():
            try:
                payload = await self._client.fetch_feed()
            except Exception:
                POLL_FAILURE_TOTAL.labels(SOURCE).inc()
                raise
            return await self._process(payload)

    async def _process(self, payload: dict[str, object]) -> PollStats:
        try:
            events, parse_errors = parse_feed(payload)
        except ValueError:
            POLL_FAILURE_TOTAL.labels(SOURCE).inc()
            raise

        stats = PollStats(fetched=len(events) + len(parse_errors))
        RECORDS_FETCHED_TOTAL.labels(SOURCE).inc(stats.fetched)

        # Structurally-broken features → data-quality audit.
        for err in parse_errors:
            self._producer.produce_data_quality(err.source_event_id, [str(err)], None)
            stats.invalid += 1

        now = utcnow()
        # (source_event_id, marker, updated_ms) for events buffered for produce this cycle.
        pending: list[tuple[str, str, int]] = []

        for event in events:
            reasons = check_event(event, now=now, future_tolerance=self._future_tolerance)
            if reasons:
                self._producer.produce_data_quality(event.source_event_id, reasons, event.payload())
                stats.invalid += 1
                continue

            marker = marker_for(event.source_version, event.payload_hash())
            if await self._cursor.seen_marker(event.source_event_id) == marker:
                stats.duplicates += 1
                continue

            self._producer.produce_event(event)
            pending.append(
                (event.source_event_id, marker, int(event.updated_time.timestamp() * 1000))
            )

        RECORDS_INVALID_TOTAL.labels(SOURCE).inc(stats.invalid)
        RECORDS_DUPLICATE_TOTAL.labels(SOURCE).inc(stats.duplicates)

        if pending or stats.invalid:
            delivered, remaining = self._producer.flush_and_confirm()
            stats.undelivered = remaining
            max_updated_ms = await self._cursor.get_cursor_ms() or 0
            for source_event_id, marker, updated_ms in pending:
                if source_event_id in delivered:
                    await self._cursor.record(source_event_id, marker)
                    stats.produced += 1
                    max_updated_ms = max(max_updated_ms, updated_ms)
                else:
                    _log.warning("produce_not_acked", source_event_id=source_event_id)
            RECORDS_PRODUCED_TOTAL.labels(SOURCE).inc(stats.produced)
            if max_updated_ms:
                await self._cursor.set_cursor_ms(max_updated_ms)
                CURSOR_LAG.labels(SOURCE).set(now.timestamp() - max_updated_ms / 1000.0)

        _log.info(
            "poll_complete",
            fetched=stats.fetched,
            produced=stats.produced,
            duplicates=stats.duplicates,
            invalid=stats.invalid,
            undelivered=stats.undelivered,
        )
        return stats
