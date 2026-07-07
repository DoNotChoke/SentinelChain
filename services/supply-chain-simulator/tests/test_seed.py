"""Unit tests for the deterministic demo seed (no database required)."""

from __future__ import annotations

from supply_chain_simulator.seed import (
    SHIPMENT_COUNT,
    build_seed_data,
    inventory_days_remaining,
    sid,
)


def test_seed_matches_demo_scenario() -> None:
    data = build_seed_data()

    assert data.supplier.country_code == "JP"
    assert data.supplier.supplier_name == "JP Semiconductor Components"
    assert data.facility.facility_name == "Yokohama Plant"
    assert len(data.shipments) == SHIPMENT_COUNT
    assert all(s.status == "in_transit" for s in data.shipments)


def test_inventory_has_about_three_days_remaining() -> None:
    data = build_seed_data()
    # 310 on hand / 100 average daily usage = 3.1 days (PLAN §35).
    assert inventory_days_remaining(data.inventory) == 3.1


def test_ids_are_stable_across_calls() -> None:
    first = build_seed_data()
    second = build_seed_data()
    assert first.supplier.supplier_id == second.supplier.supplier_id
    assert first.facility.facility_id == second.facility.facility_id
    assert [s.shipment_id for s in first.shipments] == [s.shipment_id for s in second.shipments]


def test_foreign_keys_are_wired() -> None:
    data = build_seed_data()
    assert data.facility.supplier_id == data.supplier.supplier_id
    assert data.purchase_order.supplier_id == data.supplier.supplier_id
    assert data.inventory.warehouse_id == data.warehouse.warehouse_id
    assert all(s.origin_facility_id == data.facility.facility_id for s in data.shipments)


def test_sid_is_deterministic() -> None:
    assert sid("supplier:jp-semiconductor") == sid("supplier:jp-semiconductor")
    assert sid("a") != sid("b")
