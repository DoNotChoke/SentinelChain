"""Cannonical event evelope shared by Kafka message"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .time import utcnow


def new_uuid() -> str:
    return str(uuid.uuid4())


class EventEnvelope(BaseModel):
    """Common envelope for all canonical events.

    Field semantics:
      - ``event_id``       unique per message (dedup is NOT done on this field).
      - ``source_event_id`` stable across upstream updates of the same logical event.
      - ``source_version``  monotonic marker used to deduplicate/version (PLAN §11.2).
      - ``event_time``      business time; ``ingested_at`` is system receipt time.
      - ``event_version``   schema evolution marker.
    """

    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=new_uuid)
    event_type: str
    event_version: int = 1
    source: str
    source_event_id: str
    source_version: str | None = None
    event_time: datetime
    ingested_at: datetime = Field(default_factory=utcnow)
    trace_id: str = Field(default_factory=new_uuid)
    correlation_id: str | None = None
    tenant_id: str = "demo"
    payload: dict[str, Any] = Field(default_factory=dict)

    def dedup_key(self) -> str:
        return f"{self.source}:{self.source_event_id}"

    def payload_hash(self) -> str:
        encoded = json.dumps(self.payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
