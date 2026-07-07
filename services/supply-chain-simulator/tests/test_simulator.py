"""Unit tests for the pure simulation decision functions (no database required)."""

from __future__ import annotations

import random
from datetime import UTC, datetime

from supply_chain_simulator.simulator import (
    SHIPMENT_STATUS_FLOW,
    advance_status,
    consume_inventory,
    drift_eta,
)


def test_consume_inventory_never_goes_negative() -> None:
    rng = random.Random(0)
    # Huge usage relative to a short tick would overshoot; must floor at 0.
    assert consume_inventory(1.0, 1_000_000.0, 2.0, rng) == 0.0


def test_consume_inventory_decreases_monotonically() -> None:
    rng = random.Random(1)
    qty = 310.0
    for _ in range(50):
        new_qty = consume_inventory(qty, 100.0, 2.0, rng)
        assert new_qty <= qty
        qty = new_qty
    assert qty < 310.0


def test_drift_eta_within_bounds() -> None:
    rng = random.Random(2)
    base = datetime(2026, 7, 2, 12, 0, tzinfo=UTC)
    for _ in range(100):
        shifted = drift_eta(base, rng)
        delta_min = (shifted - base).total_seconds() / 60.0
        assert -30.0 <= delta_min <= 90.0


def test_advance_status_follows_flow() -> None:
    rng = random.Random(3)
    assert advance_status("created", rng, probability=1.0) == "in_transit"
    assert advance_status("in_transit", rng, probability=1.0) == "customs"
    # Terminal state never advances even at probability 1.
    assert advance_status("delivered", rng, probability=1.0) == "delivered"


def test_advance_status_can_stay() -> None:
    rng = random.Random(4)
    assert advance_status("created", rng, probability=0.0) == "created"


def test_status_flow_terminates_at_delivered() -> None:
    status = "created"
    seen = [status]
    while (nxt := SHIPMENT_STATUS_FLOW[status]) is not None:
        status = nxt
        seen.append(status)
    assert seen[-1] == "delivered"
