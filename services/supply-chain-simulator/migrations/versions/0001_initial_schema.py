from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision = "0001_initial_schema"
down_revision = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CDC_TABLES: tuple[str, ...] = (
    "suppliers",
    "facilities",
    "shipments",
    "inventory",
    "purchase_orders",
)


def upgrade() -> None:
    op.create_table(
        "suppliers",
        sa.Column("supplier_id", sa.Uuid(), primary_key=True),
        sa.Column("supplier_name", sa.Text(), nullable=False),
        sa.Column("normalized_name", sa.Text(), nullable=False),
        sa.Column("country_code", sa.CHAR(2)),
        sa.Column("supplier_tier", sa.Integer(), nullable=False),
        sa.Column("criticality_score", sa.Numeric(5, 4), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "products",
        sa.Column("product_id", sa.Uuid(), primary_key=True),
        sa.Column("product_name", sa.Text(), nullable=False),
        sa.Column("category", sa.Text()),
        sa.Column("criticality_score", sa.Numeric(5, 4), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "warehouses",
        sa.Column("warehouse_id", sa.Uuid(), primary_key=True),
        sa.Column("warehouse_name", sa.Text(), nullable=False),
        sa.Column("latitude", sa.Double(), nullable=False),
        sa.Column("longitude", sa.Double(), nullable=False),
        sa.Column("country_code", sa.CHAR(2)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "facilities",
        sa.Column("facility_id", sa.Uuid(), primary_key=True),
        sa.Column(
            "supplier_id",
            sa.Uuid(),
            sa.ForeignKey("suppliers.supplier_id"),
            nullable=False,
        ),
        sa.Column("facility_name", sa.Text(), nullable=False),
        sa.Column("facility_type", sa.Text(), nullable=False),
        sa.Column("latitude", sa.Double(), nullable=False),
        sa.Column("longitude", sa.Double(), nullable=False),
        sa.Column("h3_index", sa.Text()),
        sa.Column("country_code", sa.CHAR(2)),
        sa.Column("capacity_score", sa.Numeric(5, 4)),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "routes",
        sa.Column("route_id", sa.Uuid(), primary_key=True),
        sa.Column(
            "origin_facility_id",
            sa.Uuid(),
            sa.ForeignKey("facilities.facility_id"),
            nullable=False,
        ),
        sa.Column(
            "destination_warehouse_id",
            sa.Uuid(),
            sa.ForeignKey("warehouses.warehouse_id"),
            nullable=False,
        ),
        sa.Column("transport_mode", sa.Text(), nullable=False),
        sa.Column("distance_km", sa.Double()),
        sa.Column("planned_transit_hours", sa.Double()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "purchase_orders",
        sa.Column("purchase_order_id", sa.Uuid(), primary_key=True),
        sa.Column(
            "supplier_id",
            sa.Uuid(),
            sa.ForeignKey("suppliers.supplier_id"),
            nullable=False,
        ),
        sa.Column(
            "product_id",
            sa.Uuid(),
            sa.ForeignKey("products.product_id"),
            nullable=False,
        ),
        sa.Column("quantity", sa.Numeric(18, 4), nullable=False),
        sa.Column("unit_price", sa.Numeric(18, 4), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("required_by_date", sa.Date()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "shipments",
        sa.Column("shipment_id", sa.Uuid(), primary_key=True),
        sa.Column("purchase_order_id", sa.Uuid(), nullable=False),
        sa.Column("supplier_id", sa.Uuid(), nullable=False),
        sa.Column("origin_facility_id", sa.Uuid(), nullable=False),
        sa.Column("destination_warehouse_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("transport_mode", sa.Text(), nullable=False),
        sa.Column("planned_departure_at", sa.DateTime(timezone=True)),
        sa.Column("planned_arrival_at", sa.DateTime(timezone=True)),
        sa.Column("estimated_arrival_at", sa.DateTime(timezone=True)),
        sa.Column("actual_arrival_at", sa.DateTime(timezone=True)),
        sa.Column("shipment_value", sa.Numeric(18, 2)),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "shipment_events",
        sa.Column("shipment_event_id", sa.Uuid(), primary_key=True),
        sa.Column(
            "shipment_id",
            sa.Uuid(),
            sa.ForeignKey("shipments.shipment_id"),
            nullable=False,
        ),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("status", sa.Text()),
        sa.Column("note", sa.Text()),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "inventory",
        sa.Column("warehouse_id", sa.Uuid(), primary_key=True),
        sa.Column("product_id", sa.Uuid(), primary_key=True),
        sa.Column("quantity_on_hand", sa.Numeric(18, 4), nullable=False),
        sa.Column("average_daily_usage", sa.Numeric(18, 4), nullable=False),
        sa.Column("safety_stock", sa.Numeric(18, 4), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_table(
        "supplier_aliases",
        sa.Column("alias", sa.Text(), primary_key=True),
        sa.Column(
            "supplier_id",
            sa.Uuid(),
            sa.ForeignKey("suppliers.supplier_id"),
            primary_key=True,
        ),
        sa.Column("alias_type", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Numeric(5, 4)),
    )

    # Debezium needs complete before-images to emit correct UPDATE/DELETE events (ADR-009).
    for table in CDC_TABLES:
        op.execute(f"ALTER TABLE {table} REPLICA IDENTITY FULL")


def downgrade() -> None:
    for table in (
        "supplier_aliases",
        "inventory",
        "shipment_events",
        "shipments",
        "purchase_orders",
        "routes",
        "facilities",
        "warehouses",
        "products",
        "suppliers",
    ):
        op.drop_table(table)
