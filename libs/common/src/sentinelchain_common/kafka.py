"""Kafka helpers"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from confluent_kafka import Message, Producer

from .config import BaseServiceSettings
from .envelope import EventEnvelope

DeliveryCallback = Callable[[Any | None, Message], None]

# Turns an envelope into the message value. Bound per topic, because the wire schema is per
# topic — see ``avro.AvroEnvelopeSerializer`` (ADR-001).
ValueSerializer = Callable[[EventEnvelope], bytes]


def producer_config(settings: BaseServiceSettings) -> dict[str, Any]:
    """Idempotent producer config.

    ``enable.idempotence`` requires ``acks=all`` and bounded in-flight requests; the client
    sets compatible defaults, which we make explicit here.
    """
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "enable.idempotence": True,
        "acks": "all",
        "max.in.flight.requests.per.connection": 5,
        "retries": 2_147_483_647,
        "compression.type": "zstd",
        "linger.ms": 20,
    }


def consumer_config(settings: BaseServiceSettings, group_id: str) -> dict[str, Any]:
    """Consumer config with manual offset commit (commit only after processing)."""
    return {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    }


class EnvelopeProducer:
    """Idempotent producer for envelope-shaped messages.

    A :data:`ValueSerializer` is supplied per topic (the value schema is per topic); the key is
    the deterministic partition key — plain UTF-8, not registry-managed, so only ``<topic>-value``
    subjects exist.
    """

    def __init__(self, settings: BaseServiceSettings) -> None:
        self._config = producer_config(settings)
        self._producer: Producer | None = None

    def _ensure(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(self._config)
        return self._producer

    def produce(
        self,
        topic: str,
        key: str,
        envelope: EventEnvelope,
        serialize: ValueSerializer,
        *,
        on_delivery: DeliveryCallback | None = None,
    ) -> None:
        """Buffer a message for delivery.

        ``on_delivery(err, msg)`` (if given) is invoked from ``flush``/``poll`` once the broker
        acknowledges or the send fails — use it to confirm an ack *before* committing a cursor
        (PLAN §11.1: never commit the cursor before Kafka acknowledges).
        """
        producer = self._ensure()
        producer.produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=serialize(envelope),
            headers={"trace_id": envelope.trace_id, "event_type": envelope.event_type},
            on_delivery=on_delivery,
        )

    def flush(self, timeout: float = 10.0) -> int:
        remaining: int = self._ensure().flush(timeout=timeout)
        return remaining
