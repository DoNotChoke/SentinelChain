"""SQLAlchemy models."""

from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import (
    CHAR,
    Date,
    DateTime,
    Double,
    ForeignKey,
    Integer,
    Numeric,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

CDC_TABLES: tuple[str, ...] = (
    "suppliers",
    "facilities",
    "shipments",
    "inventory",
    "purchase_orders",
)


class Base(DeclarativeBase):
    pass


class Supplier(Base):
    __tablename__ = "suppliers"

    supplier_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    supplier_name: Mapped[str] = mapped_column(Text, nullable=False)
    normalized_name: Mapped[str] = mapped_column(Text, nullable=False)
    country_code: Mapped[str | None] = mapped_column(CHAR(2))
    supplier_tier: Mapped[int] = mapped_column(Integer, nullable=False)
    criticality_score: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Facility(Base):
    __tablename__ = "facilities"

    facility_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    supplier_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("suppliers.supplier_id"), nullable=False
    )
    facility_name: Mapped[str] = mapped_column(Text, nullable=False)
    facility_type: Mapped[str] = mapped_column(Text, nullable=False)
    latitude: Mapped[float] = mapped_column(Double, nullable=False)
    longitude: Mapped[float] = mapped_column(Double, nullable=False)
    h3_index: Mapped[str | None] = mapped_column(Text)
    country_code: Mapped[str | None] = mapped_column(CHAR(2))
    capacity_score: Mapped[float | None] = mapped_column(Numeric(5, 4))
    status: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    product_name: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str | None] = mapped_column(Text)
    criticality_score: Mapped[float] = mapped_column(Numeric(5, 4), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Warehouse(Base):
    __tablename__ = "warehouses"

    warehouse_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    warehouse_name: Mapped[str] = mapped_column(Text, nullable=False)
    latitude: Mapped[float] = mapped_column(Double, nullable=False)
    longitude: Mapped[float] = mapped_column(Double, nullable=False)
    country_code: Mapped[str | None] = mapped_column(CHAR(2))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Route(Base):
    __tablename__ = "routes"

    route_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    origin_facility_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("facilities.facility_id"), nullable=False
    )
    destination_warehouse_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("warehouses.warehouse_id"), nullable=False
    )
    transport_mode: Mapped[str] = mapped_column(Text, nullable=False)
    distance_km: Mapped[float | None] = mapped_column(Double)
    planned_transit_hours: Mapped[float | None] = mapped_column(Double)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PurchaseOrder(Base):
    __tablename__ = "purchase_orders"

    purchase_order_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    supplier_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("suppliers.supplier_id"), nullable=False
    )
    product_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("products.product_id"), nullable=False)
    quantity: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    unit_price: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    required_by_date: Mapped[date | None] = mapped_column(Date)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Shipment(Base):
    __tablename__ = "shipments"

    shipment_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    purchase_order_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    supplier_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    origin_facility_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    destination_warehouse_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    transport_mode: Mapped[str] = mapped_column(Text, nullable=False)
    planned_departure_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    planned_arrival_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    estimated_arrival_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    actual_arrival_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    shipment_value: Mapped[float | None] = mapped_column(Numeric(18, 2))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ShipmentEvent(Base):
    __tablename__ = "shipment_events"

    shipment_event_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    shipment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("shipments.shipment_id"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str | None] = mapped_column(Text)
    note: Mapped[str | None] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class Inventory(Base):
    __tablename__ = "inventory"

    warehouse_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    product_id: Mapped[uuid.UUID] = mapped_column(primary_key=True)
    quantity_on_hand: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    average_daily_usage: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    safety_stock: Mapped[float] = mapped_column(Numeric(18, 4), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class SupplierAlias(Base):
    __tablename__ = "supplier_aliases"

    alias: Mapped[str] = mapped_column(Text, primary_key=True)
    supplier_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("suppliers.supplier_id"), primary_key=True
    )
    alias_type: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[float | None] = mapped_column(Numeric(5, 4))
