-- =============================================================================
-- Bronze Schema: Raw Data Ingestion Layer
-- =============================================================================
-- Bronze tables are immutable/append-only. Each table stores raw source data
-- as JSONB payloads with metadata for lineage tracking and deduplication.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;

-- =============================================================================
-- Helper: pgcrypto extension for MD5 hashing (if not already available)
-- =============================================================================
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =============================================================================
-- OPERATIONAL TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.raw_customers
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_customers (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_customers_source_row_id
    ON bronze.raw_customers (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_customers_checksum
    ON bronze.raw_customers (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_customers_row_run
    ON bronze.raw_customers (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_customer_profiles
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_customer_profiles (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_customer_profiles_source_row_id
    ON bronze.raw_customer_profiles (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_customer_profiles_checksum
    ON bronze.raw_customer_profiles (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_customer_profiles_row_run
    ON bronze.raw_customer_profiles (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_customer_addresses
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_customer_addresses (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_customer_addresses_source_row_id
    ON bronze.raw_customer_addresses (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_customer_addresses_checksum
    ON bronze.raw_customer_addresses (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_customer_addresses_row_run
    ON bronze.raw_customer_addresses (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_products
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_products (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_products_source_row_id
    ON bronze.raw_products (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_products_checksum
    ON bronze.raw_products (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_products_row_run
    ON bronze.raw_products (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_product_categories
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_product_categories (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_product_categories_source_row_id
    ON bronze.raw_product_categories (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_product_categories_checksum
    ON bronze.raw_product_categories (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_product_categories_row_run
    ON bronze.raw_product_categories (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_stores
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_stores (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_stores_source_row_id
    ON bronze.raw_stores (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_stores_checksum
    ON bronze.raw_stores (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_stores_row_run
    ON bronze.raw_stores (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_sales_channels
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_sales_channels (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_sales_channels_source_row_id
    ON bronze.raw_sales_channels (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_sales_channels_checksum
    ON bronze.raw_sales_channels (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_sales_channels_row_run
    ON bronze.raw_sales_channels (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_promotions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_promotions (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_promotions_source_row_id
    ON bronze.raw_promotions (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_promotions_checksum
    ON bronze.raw_promotions (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_promotions_row_run
    ON bronze.raw_promotions (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_orders
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_orders_source_row_id
    ON bronze.raw_orders (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_checksum
    ON bronze.raw_orders (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_orders_row_run
    ON bronze.raw_orders (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_order_items
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_order_items (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_order_items_source_row_id
    ON bronze.raw_order_items (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_order_items_checksum
    ON bronze.raw_order_items (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_order_items_row_run
    ON bronze.raw_order_items (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_order_promotions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_order_promotions (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_order_promotions_source_row_id
    ON bronze.raw_order_promotions (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_order_promotions_checksum
    ON bronze.raw_order_promotions (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_order_promotions_row_run
    ON bronze.raw_order_promotions (source_row_id, pipeline_run_id);

-- =============================================================================
-- EVENT TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.raw_payment_events
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_payment_events (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_payment_events_source_row_id
    ON bronze.raw_payment_events (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_payment_events_checksum
    ON bronze.raw_payment_events (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_payment_events_row_run
    ON bronze.raw_payment_events (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_refund_events
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_refund_events (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_refund_events_source_row_id
    ON bronze.raw_refund_events (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_refund_events_checksum
    ON bronze.raw_refund_events (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_refund_events_row_run
    ON bronze.raw_refund_events (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_return_events
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_return_events (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_return_events_source_row_id
    ON bronze.raw_return_events (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_return_events_checksum
    ON bronze.raw_return_events (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_return_events_row_run
    ON bronze.raw_return_events (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_support_events
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_support_events (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_support_events_source_row_id
    ON bronze.raw_support_events (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_support_events_checksum
    ON bronze.raw_support_events (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_support_events_row_run
    ON bronze.raw_support_events (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_web_events
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_web_events (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_web_events_source_row_id
    ON bronze.raw_web_events (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_web_events_checksum
    ON bronze.raw_web_events (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_web_events_row_run
    ON bronze.raw_web_events (source_row_id, pipeline_run_id);

-- =============================================================================
-- INVENTORY TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.raw_inventory_snapshots
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_inventory_snapshots (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_inventory_snapshots_source_row_id
    ON bronze.raw_inventory_snapshots (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_inventory_snapshots_checksum
    ON bronze.raw_inventory_snapshots (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_inventory_snapshots_row_run
    ON bronze.raw_inventory_snapshots (source_row_id, pipeline_run_id);

-- =============================================================================
-- REFERENCE TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- bronze.raw_campaign_spend
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_campaign_spend (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_campaign_spend_source_row_id
    ON bronze.raw_campaign_spend (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_campaign_spend_checksum
    ON bronze.raw_campaign_spend (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_campaign_spend_row_run
    ON bronze.raw_campaign_spend (source_row_id, pipeline_run_id);

-- -----------------------------------------------------------------------------
-- bronze.raw_city_reference
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.raw_city_reference (
    bronze_id       BIGSERIAL       PRIMARY KEY,
    source_file     TEXT            NOT NULL,
    source_row_num  INTEGER,
    source_row_id   TEXT,
    raw_payload     JSONB           NOT NULL,
    checksum        TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_city_reference_source_row_id
    ON bronze.raw_city_reference (source_row_id);
CREATE INDEX IF NOT EXISTS idx_raw_city_reference_checksum
    ON bronze.raw_city_reference (checksum);
CREATE UNIQUE INDEX IF NOT EXISTS uq_raw_city_reference_row_run
    ON bronze.raw_city_reference (source_row_id, pipeline_run_id);

-- =============================================================================
-- End of Bronze Schema
-- =============================================================================
