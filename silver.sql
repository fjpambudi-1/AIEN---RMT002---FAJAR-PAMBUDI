-- ============================================================
-- Silver Schema — Typed, Normalized Tables
-- ============================================================
-- Silver tables extract fields from Bronze JSONB payloads into
-- proper columns with enforced types, constraints, and indexes.
-- SCD2 tables track historical changes via valid_from/valid_to.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS silver;

-- ------------------------------------------------------------
-- Operational Tables
-- ------------------------------------------------------------

-- Customers (current state)
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id         TEXT            PRIMARY KEY,
    city_id             TEXT,
    customer_segment    TEXT,
    created_at_utc      TIMESTAMPTZ     NOT NULL,
    source_row_id       TEXT            NOT NULL,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_customers_city_id
    ON silver.customers (city_id);
CREATE INDEX IF NOT EXISTS idx_customers_segment
    ON silver.customers (customer_segment);
CREATE INDEX IF NOT EXISTS idx_customers_bronze_id
    ON silver.customers (bronze_id);

-- Customer Profiles (SCD2 — tracks segment / city changes)
CREATE TABLE IF NOT EXISTS silver.customer_profiles (
    profile_id          BIGSERIAL       PRIMARY KEY,
    customer_id         TEXT            NOT NULL,
    city_id             TEXT,
    customer_segment    TEXT,
    valid_from_utc      TIMESTAMPTZ     NOT NULL,
    valid_to_utc        TIMESTAMPTZ,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_customer_profiles_customer_id
    ON silver.customer_profiles (customer_id);
CREATE INDEX IF NOT EXISTS idx_customer_profiles_valid_range
    ON silver.customer_profiles (customer_id, valid_from_utc, valid_to_utc);
CREATE INDEX IF NOT EXISTS idx_customer_profiles_bronze_id
    ON silver.customer_profiles (bronze_id);

-- Customer Addresses (SCD2)
CREATE TABLE IF NOT EXISTS silver.customer_addresses (
    address_id          TEXT            PRIMARY KEY,
    customer_id         TEXT            NOT NULL,
    city_id             TEXT,
    valid_from_utc      TIMESTAMPTZ     NOT NULL,
    valid_to_utc        TIMESTAMPTZ,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_customer_addresses_customer_id
    ON silver.customer_addresses (customer_id);
CREATE INDEX IF NOT EXISTS idx_customer_addresses_city_id
    ON silver.customer_addresses (city_id);
CREATE INDEX IF NOT EXISTS idx_customer_addresses_bronze_id
    ON silver.customer_addresses (bronze_id);

-- Products
CREATE TABLE IF NOT EXISTS silver.products (
    product_id          TEXT            PRIMARY KEY,
    product_name        TEXT,
    sku                 TEXT,
    unit_price          NUMERIC(14,2)   NOT NULL,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_products_sku
    ON silver.products (sku);
CREATE INDEX IF NOT EXISTS idx_products_bronze_id
    ON silver.products (bronze_id);

-- Product Categories (SCD2)
CREATE TABLE IF NOT EXISTS silver.product_categories (
    category_record_id  BIGSERIAL       PRIMARY KEY,
    product_id          TEXT            NOT NULL,
    category_id         TEXT,
    category_name       TEXT,
    valid_from_utc      TIMESTAMPTZ     NOT NULL,
    valid_to_utc        TIMESTAMPTZ,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_product_categories_product_id
    ON silver.product_categories (product_id);
CREATE INDEX IF NOT EXISTS idx_product_categories_category_id
    ON silver.product_categories (category_id);
CREATE INDEX IF NOT EXISTS idx_product_categories_bronze_id
    ON silver.product_categories (bronze_id);

-- Stores
CREATE TABLE IF NOT EXISTS silver.stores (
    store_id            TEXT            PRIMARY KEY,
    store_name          TEXT,
    city_id             TEXT,
    location_type       TEXT,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stores_city_id
    ON silver.stores (city_id);
CREATE INDEX IF NOT EXISTS idx_stores_bronze_id
    ON silver.stores (bronze_id);

-- Sales Channels
CREATE TABLE IF NOT EXISTS silver.sales_channels (
    channel_id          TEXT            PRIMARY KEY,
    channel_name        TEXT,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sales_channels_bronze_id
    ON silver.sales_channels (bronze_id);

-- Promotions
CREATE TABLE IF NOT EXISTS silver.promotions (
    promotion_id        TEXT            PRIMARY KEY,
    promotion_code      TEXT,
    promotion_type      TEXT,
    discount_rate       NUMERIC(14,4),
    start_date          DATE,
    end_date            DATE,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_promotions_date_range
    ON silver.promotions (start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_promotions_bronze_id
    ON silver.promotions (bronze_id);

-- Orders
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id            TEXT            PRIMARY KEY,
    customer_id         TEXT            NOT NULL,
    ordered_at_utc      TIMESTAMPTZ     NOT NULL,
    updated_at_utc      TIMESTAMPTZ,
    sales_channel       TEXT            NOT NULL,
    store_id            TEXT,
    status              TEXT            NOT NULL,
    shipping_revenue    NUMERIC(14,2)   NOT NULL DEFAULT 0,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON silver.orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_ordered_at
    ON silver.orders (ordered_at_utc);
CREATE INDEX IF NOT EXISTS idx_orders_store_id
    ON silver.orders (store_id);
CREATE INDEX IF NOT EXISTS idx_orders_status
    ON silver.orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_sales_channel
    ON silver.orders (sales_channel);
CREATE INDEX IF NOT EXISTS idx_orders_bronze_id
    ON silver.orders (bronze_id);

-- Order Items
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id       TEXT            PRIMARY KEY,
    order_id            TEXT            NOT NULL,
    product_id          TEXT            NOT NULL,
    quantity            INTEGER         NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(14,2)   NOT NULL,
    item_discount_amount NUMERIC(14,2)  NOT NULL DEFAULT 0,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON silver.order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON silver.order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_bronze_id
    ON silver.order_items (bronze_id);

-- Order Promotions
CREATE TABLE IF NOT EXISTS silver.order_promotions (
    order_promo_id      BIGSERIAL       PRIMARY KEY,
    order_id            TEXT            NOT NULL,
    promotion_id        TEXT            NOT NULL,
    discount_amount     NUMERIC(14,2)   NOT NULL DEFAULT 0,
    source_row_id       TEXT            NOT NULL UNIQUE,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_promotions_order_id
    ON silver.order_promotions (order_id);
CREATE INDEX IF NOT EXISTS idx_order_promotions_promotion_id
    ON silver.order_promotions (promotion_id);
CREATE INDEX IF NOT EXISTS idx_order_promotions_bronze_id
    ON silver.order_promotions (bronze_id);

-- ------------------------------------------------------------
-- Event Tables (flattened from nested Bronze payloads)
-- ------------------------------------------------------------

-- Payment Events
CREATE TABLE IF NOT EXISTS silver.payment_events (
    event_id            TEXT            PRIMARY KEY,
    event_type          TEXT            NOT NULL,
    occurred_at_utc     TIMESTAMPTZ     NOT NULL,
    ingested_at_utc     TIMESTAMPTZ,
    payment_id          TEXT            NOT NULL,
    order_id            TEXT            NOT NULL,
    amount              NUMERIC(14,2)   NOT NULL,
    source_row_id       TEXT,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_payment_events_order_id
    ON silver.payment_events (order_id);
CREATE INDEX IF NOT EXISTS idx_payment_events_payment_id
    ON silver.payment_events (payment_id);
CREATE INDEX IF NOT EXISTS idx_payment_events_occurred_at
    ON silver.payment_events (occurred_at_utc);
CREATE INDEX IF NOT EXISTS idx_payment_events_bronze_id
    ON silver.payment_events (bronze_id);

-- Refund Events
CREATE TABLE IF NOT EXISTS silver.refund_events (
    event_id            TEXT            PRIMARY KEY,
    event_type          TEXT            NOT NULL,
    occurred_at_utc     TIMESTAMPTZ     NOT NULL,
    ingested_at_utc     TIMESTAMPTZ,
    refund_id           TEXT            NOT NULL,
    order_id            TEXT            NOT NULL,
    amount              NUMERIC(14,2)   NOT NULL,
    source_row_id       TEXT,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_refund_events_order_id
    ON silver.refund_events (order_id);
CREATE INDEX IF NOT EXISTS idx_refund_events_refund_id
    ON silver.refund_events (refund_id);
CREATE INDEX IF NOT EXISTS idx_refund_events_occurred_at
    ON silver.refund_events (occurred_at_utc);
CREATE INDEX IF NOT EXISTS idx_refund_events_bronze_id
    ON silver.refund_events (bronze_id);

-- Return Events
CREATE TABLE IF NOT EXISTS silver.return_events (
    event_id            TEXT            PRIMARY KEY,
    event_type          TEXT            NOT NULL,
    occurred_at_utc     TIMESTAMPTZ     NOT NULL,
    ingested_at_utc     TIMESTAMPTZ,
    return_id           TEXT            NOT NULL,
    order_id            TEXT            NOT NULL,
    source_row_id       TEXT,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_return_events_order_id
    ON silver.return_events (order_id);
CREATE INDEX IF NOT EXISTS idx_return_events_return_id
    ON silver.return_events (return_id);
CREATE INDEX IF NOT EXISTS idx_return_events_occurred_at
    ON silver.return_events (occurred_at_utc);
CREATE INDEX IF NOT EXISTS idx_return_events_bronze_id
    ON silver.return_events (bronze_id);

-- Support Events
CREATE TABLE IF NOT EXISTS silver.support_events (
    event_id            TEXT            PRIMARY KEY,
    event_type          TEXT            NOT NULL,
    occurred_at_utc     TIMESTAMPTZ     NOT NULL,
    ingested_at_utc     TIMESTAMPTZ,
    ticket_id           TEXT            NOT NULL,
    order_id            TEXT,
    customer_id         TEXT            NOT NULL,
    reason              TEXT,
    source_row_id       TEXT,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_support_events_customer_id
    ON silver.support_events (customer_id);
CREATE INDEX IF NOT EXISTS idx_support_events_order_id
    ON silver.support_events (order_id);
CREATE INDEX IF NOT EXISTS idx_support_events_ticket_id
    ON silver.support_events (ticket_id);
CREATE INDEX IF NOT EXISTS idx_support_events_occurred_at
    ON silver.support_events (occurred_at_utc);
CREATE INDEX IF NOT EXISTS idx_support_events_bronze_id
    ON silver.support_events (bronze_id);

-- Web Events
CREATE TABLE IF NOT EXISTS silver.web_events (
    event_id            TEXT            PRIMARY KEY,
    event_type          TEXT            NOT NULL,
    occurred_at_utc     TIMESTAMPTZ     NOT NULL,
    ingested_at_utc     TIMESTAMPTZ,
    session_id          TEXT,
    customer_id         TEXT,
    order_id            TEXT,
    channel             TEXT,
    campaign_id         TEXT,
    source_row_id       TEXT,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_web_events_customer_id
    ON silver.web_events (customer_id);
CREATE INDEX IF NOT EXISTS idx_web_events_session_id
    ON silver.web_events (session_id);
CREATE INDEX IF NOT EXISTS idx_web_events_order_id
    ON silver.web_events (order_id);
CREATE INDEX IF NOT EXISTS idx_web_events_campaign_id
    ON silver.web_events (campaign_id);
CREATE INDEX IF NOT EXISTS idx_web_events_occurred_at
    ON silver.web_events (occurred_at_utc);
CREATE INDEX IF NOT EXISTS idx_web_events_bronze_id
    ON silver.web_events (bronze_id);

-- ------------------------------------------------------------
-- Other Tables
-- ------------------------------------------------------------

-- Inventory Snapshots
CREATE TABLE IF NOT EXISTS silver.inventory_snapshots (
    snapshot_id         BIGSERIAL       PRIMARY KEY,
    product_id          TEXT            NOT NULL,
    location_id         TEXT            NOT NULL,
    snapshot_date       DATE            NOT NULL,
    available_quantity  INTEGER         NOT NULL,
    reserved_quantity   INTEGER         NOT NULL,
    unit_cost           NUMERIC(14,2),
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL,
    UNIQUE (product_id, location_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_inventory_snapshots_product_id
    ON silver.inventory_snapshots (product_id);
CREATE INDEX IF NOT EXISTS idx_inventory_snapshots_location_id
    ON silver.inventory_snapshots (location_id);
CREATE INDEX IF NOT EXISTS idx_inventory_snapshots_date
    ON silver.inventory_snapshots (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_inventory_snapshots_bronze_id
    ON silver.inventory_snapshots (bronze_id);

-- Campaign Spend
CREATE TABLE IF NOT EXISTS silver.campaign_spend (
    spend_id            BIGSERIAL       PRIMARY KEY,
    spend_date          DATE            NOT NULL,
    campaign_id         TEXT            NOT NULL,
    channel             TEXT            NOT NULL,
    spend_amount        NUMERIC(14,2)   NOT NULL,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL,
    UNIQUE (spend_date, campaign_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_campaign_spend_campaign_id
    ON silver.campaign_spend (campaign_id);
CREATE INDEX IF NOT EXISTS idx_campaign_spend_date
    ON silver.campaign_spend (spend_date);
CREATE INDEX IF NOT EXISTS idx_campaign_spend_channel
    ON silver.campaign_spend (channel);
CREATE INDEX IF NOT EXISTS idx_campaign_spend_bronze_id
    ON silver.campaign_spend (bronze_id);

-- City Reference
CREATE TABLE IF NOT EXISTS silver.city_reference (
    city_id             TEXT            PRIMARY KEY,
    city_name           TEXT,
    country             TEXT,
    bronze_id           BIGINT          REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_city_reference_country
    ON silver.city_reference (country);
CREATE INDEX IF NOT EXISTS idx_city_reference_bronze_id
    ON silver.city_reference (bronze_id);

-- ------------------------------------------------------------
-- Rejected Records
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejection_id        BIGSERIAL       PRIMARY KEY,
    source_table        TEXT            NOT NULL,
    source_row_id       TEXT,
    bronze_id           BIGINT,
    rejection_reason    TEXT            NOT NULL,
    raw_payload         JSONB,
    rejected_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    pipeline_run_id     TEXT            NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rejected_records_source_table
    ON silver.rejected_records (source_table);
CREATE INDEX IF NOT EXISTS idx_rejected_records_pipeline_run
    ON silver.rejected_records (pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_rejected_records_rejected_at
    ON silver.rejected_records (rejected_at);

-- ============================================================
-- Additional Silver Layer Enhancements to meet requirements
-- ============================================================

-- 1. Order status enum for normalized status values
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_status') THEN
        CREATE TYPE order_status AS ENUM ('pending','processing','shipped','cancelled','returned');
    END IF;
END $$;

-- Alter orders.status to use the enum (if not already converted)
ALTER TABLE silver.orders
    ALTER COLUMN status TYPE order_status USING status::order_status;

-- 2. Foreign key constraints between Silver tables
ALTER TABLE silver.orders
    ADD CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES silver.customers(customer_id),
    ADD CONSTRAINT fk_orders_store FOREIGN KEY (store_id) REFERENCES silver.stores(store_id),
    ADD CONSTRAINT fk_orders_sales_channel FOREIGN KEY (sales_channel) REFERENCES silver.sales_channels(channel_id);

ALTER TABLE silver.order_items
    ADD CONSTRAINT fk_items_order FOREIGN KEY (order_id) REFERENCES silver.orders(order_id),
    ADD CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES silver.products(product_id);

ALTER TABLE silver.order_promotions
    ADD CONSTRAINT fk_orderpromo_order FOREIGN KEY (order_id) REFERENCES silver.orders(order_id),
    ADD CONSTRAINT fk_orderpromo_promo FOREIGN KEY (promotion_id) REFERENCES silver.promotions(promotion_id);

-- 3. Rejected records tracking table
CREATE TABLE IF NOT EXISTS silver.rejected_records (
    rejected_id   BIGSERIAL PRIMARY KEY,
    source_row_id TEXT NOT NULL,
    bronze_id     BIGINT REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id TEXT NOT NULL,
    reason        TEXT NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 4. Event identity resolution table (optional but useful)
CREATE TABLE IF NOT EXISTS silver.event_identity (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_row_id   TEXT NOT NULL UNIQUE,
    bronze_id       BIGINT REFERENCES bronze.raw_ingestion(id),
    pipeline_run_id TEXT NOT NULL,
    created_at_utc  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 5. Late‑arriving record handling – add ingestion timestamp to core tables
ALTER TABLE silver.customers ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.customer_profiles ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.customer_addresses ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.products ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.product_categories ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.stores ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.sales_channels ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.promotions ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.orders ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.order_items ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE silver.order_promotions ADD COLUMN IF NOT EXISTS ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- End of enhancements
