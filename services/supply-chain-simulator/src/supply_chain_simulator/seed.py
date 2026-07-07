"""Deterministic demo seed (PLAN §35).

Builds the reproducible scenario the end-to-end demo asserts on: a Japanese supplier, a facility
near Tokyo, a destination warehouse, a product, an open purchase order, 5 active shipments, and
inventory with ~3.1 days remaining. All ids are derived with :func:`uuid.uuid5` from a fixed
namespace so every run produces the same keys — and re-seeding is idempotent (``session.merge``).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import timedelta

from sqlalchemy.orm import Session

from sentinelchain_common import utcnow

from .models import (
    Facility,
    Inventory,
    Product,
    PurchaseOrder,
    Shipment,
    Supplier,
    SupplierAlias,
    Warehouse,
)

# Fixed namespace so seed ids are stable across runs/machines.
_NS = uuid.UUID("5e27100c-0000-4000-8000-000000000001")

SHIPMENT_COUNT = 5


def sid(name: str) -> uuid.UUID:
    """Stable UUID for a logical seed entity name."""
    return uuid.uuid5(_NS, name)


@dataclass(slots=True)
class SeedData:
    supplier: Supplier
    facility: Facility
    warehouse: Warehouse
    product: Product
    purchase_order: PurchaseOrder
    inventory: Inventory
    shipments: list[Shipment] = field(default_factory=list)
    aliases: list[SupplierAlias] = field(default_factory=list)


def build_seed_data() -> SeedData:
    """Construct (but do not persist) the deterministic demo scenario."""
    now = utcnow()

    supplier = Supplier(
        supplier_id=sid("supplier:jp-semiconductor"),
        supplier_name="JP Semiconductor Components",
        normalized_name="jp semiconductor components",
        country_code="JP",
        supplier_tier=1,
        criticality_score=0.95,
        status="active",
        created_at=now,
        updated_at=now,
    )

    facility = Facility(
        facility_id=sid("facility:yokohama-plant"),
        supplier_id=supplier.supplier_id,
        facility_name="Yokohama Plant",
        facility_type="factory",
        latitude=35.4437,
        longitude=139.6380,
        h3_index=None,
        country_code="JP",
        capacity_score=0.90,
        status="active",
        created_at=now,
        updated_at=now,
    )

    warehouse = Warehouse(
        warehouse_id=sid("warehouse:osaka-dc"),
        warehouse_name="Osaka Distribution Center",
        latitude=34.6937,
        longitude=135.5023,
        country_code="JP",
        created_at=now,
        updated_at=now,
    )

    product = Product(
        product_id=sid("product:sc-wafer-300mm"),
        product_name="300mm Silicon Wafer",
        category="semiconductor",
        criticality_score=0.90,
        created_at=now,
        updated_at=now,
    )

    purchase_order = PurchaseOrder(
        purchase_order_id=sid("po:demo-0001"),
        supplier_id=supplier.supplier_id,
        product_id=product.product_id,
        quantity=10000,
        unit_price=120,
        status="open",
        required_by_date=(now + timedelta(days=14)).date(),
        created_at=now,
        updated_at=now,
    )

    # Inventory: 310 on hand / 100 average daily usage => 3.1 days remaining (PLAN §35).
    inventory = Inventory(
        warehouse_id=warehouse.warehouse_id,
        product_id=product.product_id,
        quantity_on_hand=310,
        average_daily_usage=100,
        safety_stock=50,
        updated_at=now,
    )

    shipments = [
        Shipment(
            shipment_id=sid(f"shipment:demo-{i:04d}"),
            purchase_order_id=purchase_order.purchase_order_id,
            supplier_id=supplier.supplier_id,
            origin_facility_id=facility.facility_id,
            destination_warehouse_id=warehouse.warehouse_id,
            status="in_transit",
            transport_mode="sea",
            planned_departure_at=now - timedelta(days=2),
            planned_arrival_at=now + timedelta(days=5),
            estimated_arrival_at=now + timedelta(days=5),
            actual_arrival_at=None,
            shipment_value=240000,
            updated_at=now,
        )
        for i in range(SHIPMENT_COUNT)
    ]

    aliases = [
        SupplierAlias(
            alias="JP Semiconductor",
            supplier_id=supplier.supplier_id,
            alias_type="short_name",
            confidence=0.95,
        ),
    ]

    return SeedData(
        supplier=supplier,
        facility=facility,
        warehouse=warehouse,
        product=product,
        purchase_order=purchase_order,
        inventory=inventory,
        shipments=shipments,
        aliases=aliases,
    )


def seed_database(session: Session) -> SeedData:
    """Idempotently persist the demo scenario. Safe to call repeatedly (merge on PK)."""
    data = build_seed_data()
    # Order respects foreign keys.
    session.merge(data.supplier)
    session.merge(data.product)
    session.merge(data.warehouse)
    session.merge(data.facility)
    session.merge(data.purchase_order)
    for shipment in data.shipments:
        session.merge(shipment)
    session.merge(data.inventory)
    for alias in data.aliases:
        session.merge(alias)
    session.commit()
    return data


def inventory_days_remaining(inv: Inventory) -> float:
    """Convenience for tests/demo: days of stock left at the current usage rate."""
    usage = float(inv.average_daily_usage)
    if usage <= 0:
        return float("inf")
    return float(inv.quantity_on_hand) / usage
