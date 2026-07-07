"""Ingestion service configuration"""

from __future__ import annotations

from sentinelchain_common import BaseServiceSettings


class IngestionUsgsSettings(BaseServiceSettings):
    service_name: str = "ingestion-usgs"

    usgs_feed_url: str = (
        "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
    )
    usgs_poll_interval_seconds: float = 60.0

    usgs_request_timeout_seconds: float = 15.0
    usgs_max_retries: int = 4

    usgs_raw_topic: str = "ext.usgs.raw.v1"
    data_quality_topic: str = "audit.data_quality.v1"

    future_event_tolerance_seconds: float = 300.0

    cursor_ttl_seconds: int = 30 * 24 * 3600

    usgs_health_port: int = 8080
