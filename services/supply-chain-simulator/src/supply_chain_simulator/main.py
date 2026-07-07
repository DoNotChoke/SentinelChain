"""Simulator entrypoint.

Startup order: configure logging/tracing → start metrics + health servers → connect to Postgres
→ optionally seed the demo scenario → run the continuous simulation loop until interrupted.
"""

from __future__ import annotations

import signal
import time
from types import FrameType

from sentinelchain_common import configure_logging, get_logger
from sentinelchain_common.observability import gauge, setup_tracing, start_metrics_server

from .config import SimulatorSettings
from .db import make_engine, make_session_factory, ping
from .health import start_health_server
from .seed import inventory_days_remaining, seed_database
from .simulator import Simulator

_UP = gauge("sim_up", "1 while the simulator process is running")


def main() -> None:
    settings = SimulatorSettings()
    configure_logging(settings.service_name, settings.log_level)
    log = get_logger(settings.service_name)
    setup_tracing(settings.service_name, settings.otel_exporter_otlp_endpoint)

    engine = make_engine(settings.postgres_dsn)
    session_factory = make_session_factory(engine)

    start_metrics_server(settings.metrics_port)
    start_health_server(settings.simulator_health_port, lambda: ping(engine))
    _UP.set(1)
    log.info(
        "simulator_starting",
        metrics_port=settings.metrics_port,
        health_port=settings.simulator_health_port,
        tick_interval_seconds=settings.simulator_tick_interval_seconds,
    )

    if settings.simulator_seed_on_start:
        with session_factory() as session:
            data = seed_database(session)
        log.info(
            "seed_applied",
            supplier=data.supplier.supplier_name,
            facility=data.facility.facility_name,
            shipments=len(data.shipments),
            inventory_days_remaining=round(inventory_days_remaining(data.inventory), 2),
        )

    if not settings.simulator_run_loop:
        log.info("seed_only_mode_exit")
        _UP.set(0)
        return

    stopping = {"flag": False}

    def _stop(_signum: int, _frame: FrameType | None) -> None:
        stopping["flag"] = True
        log.info("simulator_stop_requested")

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    simulator = Simulator(session_factory, settings)
    log.info("simulator_loop_started")
    while not stopping["flag"]:
        try:
            with session_factory() as session:
                writes = simulator.tick(session)
            log.debug("tick_complete", writes=writes)
        except Exception:
            log.exception("tick_failed")
        time.sleep(settings.simulator_tick_interval_seconds)

    _UP.set(0)
    log.info("simulator_stopped")


if __name__ == "__main__":
    main()
