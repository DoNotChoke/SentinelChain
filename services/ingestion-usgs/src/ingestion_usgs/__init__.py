"""USGS earthquake ingestion service.

Polls the USGS earthquake GeoJSON feed on a fixed interval, deduplicates against a persisted
cursor, and produces canonical envelopes into ``ext.usgs.raw.v1`` with an idempotent producer.
See docs/IMPLEMENTATION_PLAN.md §E Phase 2 and PLAN §7.1 / §11.1.
"""

from __future__ import annotations

__version__ = "0.1.0"
