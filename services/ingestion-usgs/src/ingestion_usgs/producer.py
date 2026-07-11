"""Kafka producer for raw USGS events and data-quality audit records.

Wraps the shared idempotent :class:`EnvelopeProducer`, serializing to **Avro** against the
Schema Registry (ADR-001). Delivery is tracked per message so the caller can persist the dedup
cursor **only for events the broker acknowledged**.
"""

from __future__ import annotations

import json
from typing import Any

from confluent_kafka import Message

from sentinelchain_common import EventEnvelope, get_logger, utcnow
from sentinelchain_common.avro import AvroEnvelopeSerializer
from sentinelchain_common.kafka import EnvelopeProducer

from .config import IngestionUsgsSettings
from .parser import UsgsEvent

_log = get_logger("ingestion-usgs.producer")

EVENT_TYPE = "usgs.earthquake.raw"
DATA_QUALITY_EVENT_TYPE = "data_quality.violation"


class RawEventProducer:
    def __init__(self, settings: IngestionUsgsSettings) -> None:
        self._producer = EnvelopeProducer(settings)
        self._raw_topic = settings.usgs_raw_topic
        self._dq_topic = settings.data_quality_topic
        # One serializer per topic: each carries its own payload schema / registry subject.
        self._raw_serializer = AvroEnvelopeSerializer(settings, self._raw_topic)
        self._dq_serializer = AvroEnvelopeSerializer(settings, self._dq_topic)
        self._delivered: set[str] = set()

    def _on_delivery(self, source_event_id: str) -> Any:
        def _cb(err: Any | None, _msg: Message) -> None:
            if err is None:
                self._delivered.add(source_event_id)
            else:
                _log.error("delivery_failed", source_event_id=source_event_id, error=str(err))

        return _cb

    def produce_event(self, event: UsgsEvent) -> None:
        """Buffer a raw USGS event envelope keyed by ``source_event_id``."""
        envelope = EventEnvelope(
            event_type=EVENT_TYPE,
            source="usgs",
            source_event_id=event.source_event_id,
            source_version=event.source_version,
            event_time=event.event_time,
            payload=event.payload(),
        )
        self._producer.produce(
            self._raw_topic,
            key=event.source_event_id,
            envelope=envelope,
            serialize=self._raw_serializer,
            on_delivery=self._on_delivery(event.source_event_id),
        )

    def produce_data_quality(
        self, source_event_id: str | None, reasons: list[str], raw: dict[str, object] | None
    ) -> None:
        """Emit a data-quality audit record for an invalid/unparseable feature (PLAN §28).

        ``raw`` is JSON-encoded rather than modelled: this topic must accept arbitrarily
        malformed records, and Avro has no 'any' type.
        """
        envelope = EventEnvelope(
            event_type=DATA_QUALITY_EVENT_TYPE,
            source="ingestion-usgs",
            source_event_id=source_event_id or "unknown",
            event_time=utcnow(),
            payload={
                "origin_source": "usgs",
                "source_event_id": source_event_id,
                "reasons": reasons,
                "raw": None if raw is None else json.dumps(raw, sort_keys=True, default=str),
            },
        )
        self._producer.produce(
            self._dq_topic,
            key=source_event_id or "unknown",
            envelope=envelope,
            serialize=self._dq_serializer,
        )

    def flush_and_confirm(self, timeout: float = 10.0) -> tuple[set[str], int]:
        """Flush buffered messages.

        Returns ``(delivered_ids, remaining)``: the set of raw-event ``source_event_id`` the
        broker acknowledged, and the number of messages still undelivered after the timeout.
        The delivered set is reset for the next cycle.
        """
        remaining = self._producer.flush(timeout=timeout)
        delivered = self._delivered
        self._delivered = set()
        return delivered, remaining
