"""Continuous operational simulation (PLAN §7.6).

Each tick applies small, realistic mutations so Debezium produces a steady stream of CDC events:
inventory is consumed, shipment ETAs drift, shipments advance through their lifecycle, and
facilities occasionally pause/resume. The *decision* logic is factored into pure functions so it
can be unit-tested without a database; :class:`Simulator` applies those decisions through a
SQLAlchemy session.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from sentinelchain_common import get_logger, utcnow
from sentinelchain_common.observability import counter, histogram

from .models import Facility, Inventory, Shipment, ShipmentEvent

_log = get_logger("supply-chain-simulator")

# ── Metrics (PLAN §26) ────────────────────────────────────────────────────────
TICKS = counter("sim_tick_total", "Simulation ticks executed")
ROW_WRITES = counter("sim_row_writes_total", "Operational row writes", ("table", "op"))
TICK_DURATION = histogram("sim_tick_duration_seconds", "Duration of a simulation tick")

# Shipment lifecycle: each status maps to its possible next status (terminal states map to None).
SHIPMENT_STATUS_FLOW: dict[str, str | None] = {
    "created": "in_transit",
    "in_transit": "customs",
    "customs": "out_for_delivery",
    "out_for_delivery": "delivered",
    "delivered": None,
}


def consume_inventory(
    quantity_on_hand: float,
    average_daily_usage: float,
    tick_seconds: float,
    rng: random.Random,
) -> float:
    """Return the new on-hand quantity after one tick of consumption.

    Consumes ``average_daily_usage`` pro-rated to the tick length with ±40% jitter, and never
    drops below zero.
    """
    fraction_of_day = tick_seconds / 86_400.0
    jitter = rng.uniform(0.6, 1.4)
    consumed = average_daily_usage * fraction_of_day * jitter
    return max(0.0, quantity_on_hand - consumed)


def drift_eta(current_eta: datetime, rng: random.Random) -> datetime:
    """Nudge an ETA by a random amount in [-30, +90] minutes (delays are more likely)."""
    return current_eta + timedelta(minutes=rng.uniform(-30.0, 90.0))


def advance_status(status: str, rng: random.Random, *, probability: float = 0.3) -> str:
    """Maybe advance a shipment to its next lifecycle status."""
    nxt = SHIPMENT_STATUS_FLOW.get(status)
    if nxt is not None and rng.random() < probability:
        return nxt
    return status


class Simulator:
    """Applies simulation decisions to the operational database."""

    def __init__(self, session_factory: type[Session] | object, settings: object) -> None:
        # session_factory is a sessionmaker; kept loosely typed to avoid a hard import here.
        self._session_factory = session_factory
        self._tick_seconds: float = getattr(settings, "simulator_tick_interval_seconds", 2.0)
        seed = getattr(settings, "simulator_random_seed", None)
        self._rng = random.Random(seed)

    def tick(self, session: Session) -> int:
        """Run one simulation tick against ``session``. Returns the number of rows written."""
        with TICK_DURATION.time():
            writes = 0
            writes += self._consume_inventory(session)
            writes += self._progress_shipments(session)
            writes += self._maybe_toggle_facility(session)
            session.commit()
        TICKS.inc()
        return writes

    def _consume_inventory(self, session: Session) -> int:
        now = utcnow()
        writes = 0
        for inv in session.scalars(select(Inventory)):
            new_qty = consume_inventory(
                float(inv.quantity_on_hand),
                float(inv.average_daily_usage),
                self._tick_seconds,
                self._rng,
            )
            inv.quantity_on_hand = round(new_qty, 4)
            inv.updated_at = now
            ROW_WRITES.labels("inventory", "update").inc()
            writes += 1
        return writes

    def _progress_shipments(self, session: Session) -> int:
        now = utcnow()
        active = list(
            session.scalars(
                select(Shipment).where(Shipment.status.not_in(("delivered", "cancelled")))
            )
        )
        if not active:
            return 0

        # Touch a random subset so not every shipment changes every tick.
        writes = 0
        for shipment in self._rng.sample(active, k=max(1, len(active) // 2)):
            if shipment.estimated_arrival_at is not None:
                shipment.estimated_arrival_at = drift_eta(shipment.estimated_arrival_at, self._rng)
            new_status = advance_status(shipment.status, self._rng)
            if new_status != shipment.status:
                shipment.status = new_status
                if new_status == "delivered":
                    shipment.actual_arrival_at = now
                session.add(
                    ShipmentEvent(
                        shipment_event_id=uuid.uuid4(),
                        shipment_id=shipment.shipment_id,
                        event_type="status_change",
                        status=new_status,
                        note=None,
                        occurred_at=now,
                        created_at=now,
                    )
                )
                ROW_WRITES.labels("shipment_events", "insert").inc()
                writes += 1
            shipment.updated_at = now
            ROW_WRITES.labels("shipments", "update").inc()
            writes += 1
        return writes

    def _maybe_toggle_facility(self, session: Session, *, probability: float = 0.05) -> int:
        if self._rng.random() >= probability:
            return 0
        facility = session.scalars(select(Facility)).first()
        if facility is None:
            return 0
        facility.status = "paused" if facility.status == "active" else "active"
        facility.updated_at = utcnow()
        ROW_WRITES.labels("facilities", "update").inc()
        return 1
