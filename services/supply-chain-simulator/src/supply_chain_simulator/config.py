"""Simulator configuration (extends the shared service settings)."""

from __future__ import annotations

from sentinelchain_common import BaseServiceSettings


class SimulatorSettings(BaseServiceSettings):
    """Settings for the supply-chain simulator.

    Inherits ``postgres_dsn``, ``metrics_port``, ``log_level`` etc. from
    :class:`BaseServiceSettings`; adds simulator-specific knobs (env prefix-free).
    """

    service_name: str = "supply-chain-simulator"

    simulator_tick_interval_seconds: float = 2.0
    simulator_seed_on_start: bool = True
    simulator_run_loop: bool = True
    simulator_random_seed: int | None = None
    simulator_health_port: int = 8080
