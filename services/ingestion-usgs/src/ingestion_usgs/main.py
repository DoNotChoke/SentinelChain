"""USGS ingestion entrypoint.

Startup order: configure logging/tracing → start metrics + health servers → build the poller →
run the async poll loop until interrupted (SIGINT/SIGTERM), flushing cleanly on exit.
"""

from __future__ import annotations

import asyncio
import contextlib

from sentinelchain_common import configure_logging, get_logger
from sentinelchain_common.observability import gauge, setup_tracing, start_metrics_server

from .client import UsgsClient
from .config import IngestionUsgsSettings
from .cursor import RedisCursorStore
from .health import start_health_server
from .poller import Poller
from .producer import RawEventProducer

_UP = gauge("ingestion_usgs_up", "1 while the USGS ingestion process is running")


async def _run(settings: IngestionUsgsSettings) -> None:
    log = get_logger(settings.service_name)

    client = UsgsClient(settings)
    producer = RawEventProducer(settings)
    cursor = RedisCursorStore(settings.redis_url, ttl_seconds=settings.cursor_ttl_seconds)
    poller = Poller(settings, client, producer, cursor)

    # Readiness reflects the last poll outcome; starts not-ready until the first success.
    state = {"ready": False}
    start_health_server(settings.usgs_health_port, lambda: state["ready"])

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        with contextlib.suppress(NotImplementedError, ValueError):
            import signal

            loop.add_signal_handler(getattr(signal, signame), stop.set)

    _UP.set(1)
    log.info(
        "ingestion_usgs_started",
        feed_url=settings.usgs_feed_url,
        poll_interval_seconds=settings.usgs_poll_interval_seconds,
        raw_topic=settings.usgs_raw_topic,
    )

    try:
        while not stop.is_set():
            try:
                await poller.poll_once()
                state["ready"] = True
            except Exception:
                state["ready"] = False
                log.exception("poll_failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=settings.usgs_poll_interval_seconds)
    finally:
        log.info("ingestion_usgs_stopping")
        producer.flush_and_confirm()
        await client.aclose()
        await cursor.aclose()
        _UP.set(0)
        log.info("ingestion_usgs_stopped")


def main() -> None:
    settings = IngestionUsgsSettings()
    configure_logging(settings.service_name, settings.log_level)
    setup_tracing(settings.service_name, settings.otel_exporter_otlp_endpoint)
    start_metrics_server(settings.metrics_port)
    asyncio.run(_run(settings))


if __name__ == "__main__":
    main()
