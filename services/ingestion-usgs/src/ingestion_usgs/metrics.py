"""Prometheus metrics for USGS ingestion (PLAN §26)."""

from __future__ import annotations

from sentinelchain_common.observability import counter, gauge, histogram

POLL_TOTAL = counter("source_poll_total", "USGS feed polls attempted", ("source",))
POLL_FAILURE_TOTAL = counter(
    "source_poll_failure_total", "USGS feed polls that failed (fetch/parse)", ("source",)
)
RECORDS_FETCHED_TOTAL = counter(
    "source_records_fetched_total", "Features returned by the USGS feed", ("source",)
)
RECORDS_PRODUCED_TOTAL = counter(
    "source_records_produced_total", "Envelopes produced to the raw topic", ("source",)
)
RECORDS_DUPLICATE_TOTAL = counter(
    "source_records_duplicate_total", "Unchanged events skipped by the cursor", ("source",)
)
RECORDS_INVALID_TOTAL = counter(
    "source_records_invalid_total", "Records routed to data-quality audit", ("source",)
)
POLL_LATENCY = histogram("source_poll_latency_seconds", "Latency of a full poll cycle", ("source",))
CURSOR_LAG = gauge(
    "source_cursor_lag_seconds",
    "Seconds between now and the newest upstream 'updated' time processed",
    ("source",),
)

SOURCE = "usgs"
