"""Health-check helpers (PLAN §23: every service exposes liveness/readiness).

A service registers async readiness checks (DB ping, Kafka reachable, ...). Liveness is
process-up; readiness aggregates the registered checks.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

ReadinessCheck = Callable[[], Awaitable[bool]]


@dataclass(slots=True)
class HealthRegistry:
    """Collects readiness checks and evaluates them concurrently."""

    _checks: dict[str, ReadinessCheck] = field(default_factory=dict)

    def register(self, name: str, check: ReadinessCheck) -> None:
        self._checks[name] = check

    def live(self) -> dict[str, str]:
        """Liveness: the process is running."""
        return {"status": "alive"}

    async def ready(self) -> tuple[bool, dict[str, bool]]:
        """Readiness: run all checks; ready iff all pass.

        A check that raises is treated as failed rather than crashing readiness.
        """
        names = list(self._checks)

        async def _run(check: ReadinessCheck) -> bool:
            try:
                return await check()
            except Exception:
                return False

        results = await asyncio.gather(*(_run(self._checks[n]) for n in names))
        detail = dict(zip(names, results, strict=True))
        return (all(results) if results else True), detail
