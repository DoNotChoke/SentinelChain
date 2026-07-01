"""Thin Kafka helpers (PLAN §33: idempotent producer, deterministic keys).

``confluent-kafka`` is an optional dependency (the ``kafka`` extra) and is imported lazily so
this package imports without it. Avro (de)serialization via Schema Registry is added in a
later milestone; Phase 0 ships config builders plus a minimal JSON producer wrapper.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .config import BaseServiceSettings
from .envelope import EventEnvelope

if TYPE_CHECKING:
    from confluent_kafka import Producer


def producer_config(settings: BaseServiceSettings) -> dict[str, Any]:
    """Idempotent producer config (PLAN §33).

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
    """Minimal keyed JSON producer for :class:`EventEnvelope` messages.

    Construct from settings, then ``produce(topic, key, envelope)``. The producer is created
    lazily so importing this module never requires ``confluent-kafka``.
    """

    def __init__(self, settings: BaseServiceSettings) -> None:
        self._config = producer_config(settings)
        self._producer: Producer | None = None

    def _ensure(self) -> Producer:
        if self._producer is None:
            from confluent_kafka import Producer  # lazy import

            self._producer = Producer(self._config)
        return self._producer

    def produce(self, topic: str, key: str, envelope: EventEnvelope) -> None:
        value = json.dumps(envelope.model_dump(mode="json"), separators=(",", ":"))
        self._ensure().produce(
            topic=topic,
            key=key.encode("utf-8"),
            value=value.encode("utf-8"),
            headers={"trace_id": envelope.trace_id, "event_type": envelope.event_type},
        )

    def flush(self, timeout: float = 10.0) -> int:
        """Block until outstanding messages are delivered. Returns # still in queue."""
        remaining: int = self._ensure().flush(timeout)
        return remaining
