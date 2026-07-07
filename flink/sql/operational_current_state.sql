-- Flink Job 3: operational-current-state (ADR-009, PLAN §18 Job 3).
--
-- Reads Debezium CDC bronze topics (`ops.public.*`, debezium-json) and materializes one
-- compacted current-state topic per entity (`ops.<entity>.current.v1`) via upsert-kafka.
-- upsert-kafka writes last-write-wins by primary key and emits a tombstone (null value) on
-- delete, so downstream jobs can rebuild current state from the compacted log — replay-safe.
--
-- Type mapping (Debezium JSON, value.converter.schemas.enable=false,
-- decimal.handling.mode=double, time.precision.mode=connect):
--   uuid/text  -> STRING
--   numeric    -> DOUBLE
--   integer    -> INT
--   timestamptz-> STRING  (already ISO-8601 UTC, e.g. "2026-07-01T17:32:44.680267Z")
--   date       -> INT     (days since epoch)
-- Timestamps are passed through as normalized UTC ISO strings; downstream jobs cast as needed.

SET 'pipeline.name' = 'operational-current-state';

-- ── Sources: Debezium CDC bronze ────────────────────────────────────────────
CREATE TABLE suppliers_cdc (
  supplier_id STRING,
  supplier_name STRING,
  normalized_name STRING,
  country_code STRING,
  supplier_tier INT,
  criticality_score DOUBLE,
  status STRING,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (supplier_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ops.public.suppliers',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-current-state-suppliers',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'false',
  'debezium-json.ignore-parse-errors' = 'false'
);

CREATE TABLE facilities_cdc (
  facility_id STRING,
  supplier_id STRING,
  facility_name STRING,
  facility_type STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  h3_index STRING,
  country_code STRING,
  capacity_score DOUBLE,
  status STRING,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (facility_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ops.public.facilities',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-current-state-facilities',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'false'
);

CREATE TABLE shipments_cdc (
  shipment_id STRING,
  purchase_order_id STRING,
  supplier_id STRING,
  origin_facility_id STRING,
  destination_warehouse_id STRING,
  status STRING,
  transport_mode STRING,
  planned_departure_at STRING,
  planned_arrival_at STRING,
  estimated_arrival_at STRING,
  actual_arrival_at STRING,
  shipment_value DOUBLE,
  updated_at STRING,
  PRIMARY KEY (shipment_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ops.public.shipments',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-current-state-shipments',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'false'
);

CREATE TABLE inventory_cdc (
  warehouse_id STRING,
  product_id STRING,
  quantity_on_hand DOUBLE,
  average_daily_usage DOUBLE,
  safety_stock DOUBLE,
  updated_at STRING,
  PRIMARY KEY (warehouse_id, product_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ops.public.inventory',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-current-state-inventory',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'false'
);

CREATE TABLE purchase_orders_cdc (
  purchase_order_id STRING,
  supplier_id STRING,
  product_id STRING,
  quantity DOUBLE,
  unit_price DOUBLE,
  status STRING,
  required_by_date INT,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (purchase_order_id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'ops.public.purchase_orders',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-current-state-purchase-orders',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'false'
);

-- ── Sinks: compacted current-state (upsert-kafka) ───────────────────────────
CREATE TABLE suppliers_current (
  supplier_id STRING,
  supplier_name STRING,
  normalized_name STRING,
  country_code STRING,
  supplier_tier INT,
  criticality_score DOUBLE,
  status STRING,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (supplier_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ops.suppliers.current.v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE facilities_current (
  facility_id STRING,
  supplier_id STRING,
  facility_name STRING,
  facility_type STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  h3_index STRING,
  country_code STRING,
  capacity_score DOUBLE,
  status STRING,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (facility_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ops.facilities.current.v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE shipments_current (
  shipment_id STRING,
  purchase_order_id STRING,
  supplier_id STRING,
  origin_facility_id STRING,
  destination_warehouse_id STRING,
  status STRING,
  transport_mode STRING,
  planned_departure_at STRING,
  planned_arrival_at STRING,
  estimated_arrival_at STRING,
  actual_arrival_at STRING,
  shipment_value DOUBLE,
  updated_at STRING,
  PRIMARY KEY (shipment_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ops.shipments.current.v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE inventory_current (
  warehouse_id STRING,
  product_id STRING,
  quantity_on_hand DOUBLE,
  average_daily_usage DOUBLE,
  safety_stock DOUBLE,
  updated_at STRING,
  PRIMARY KEY (warehouse_id, product_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ops.inventory.current.v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE purchase_orders_current (
  purchase_order_id STRING,
  supplier_id STRING,
  product_id STRING,
  quantity DOUBLE,
  unit_price DOUBLE,
  status STRING,
  required_by_date INT,
  created_at STRING,
  updated_at STRING,
  PRIMARY KEY (purchase_order_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ops.purchase_orders.current.v1',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- ── One job, five sinks ──────────────────────────────────────────────────────
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO suppliers_current SELECT * FROM suppliers_cdc;
  INSERT INTO facilities_current SELECT * FROM facilities_cdc;
  INSERT INTO shipments_current SELECT * FROM shipments_cdc;
  INSERT INTO inventory_current SELECT * FROM inventory_cdc;
  INSERT INTO purchase_orders_current SELECT * FROM purchase_orders_cdc;
END;
