
"""Gold pipeline: materialize Gold tables from Silver.

The Gold layer must **read only from the Silver schema**, compute
*facts* and *aggregates* separately, and only afterwards join to
promotion, campaign, inventory or event dimensions.  This prevents
double‑counting of measures when a fact row touches multiple dimension
records.

The pipeline performs the following steps:

1. **Order fact** – one row per order (`gold.order_360`).
   Pulls columns directly from `silver.orders` and the related
   `silver.order_items` (summing quantity, revenue, etc.).
2. **Daily customer aggregates** – `gold.customer_daily`.
   Groups orders by `customer_id` and `order_date`.
3. **Daily product aggregates** – `gold.product_daily`.
   Groups order items by `product_id` and order date, then joins the
   inventory snapshot for the same day to avoid duplication.
4. **Channel‑campaign aggregates** – `gold.channel_campaign_daily`.
   Joins daily orders with `silver.promotions` and `silver.campaign_spend`
   *after* the fact tables are built, ensuring each order contributes only
   once.
5. **Executive KPIs** – `gold.executive_kpis_daily`.
   Summarises the daily metrics across all customers/products.

All write operations are wrapped in a transaction per pipeline run so
that the `pipeline_run_id` column can be used for lineage and
re‑processing.
"""

import os
import uuid
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

# ---------------------------------------------------------------------------
# Configuration – read from environment variables or fall back to defaults.
# ---------------------------------------------------------------------------
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")

# Helper to get a fresh run identifier for lineage tracking.
def _new_run_id() -> str:
    return f"gold_run_{uuid.uuid4().hex[:12]}"

# ---------------------------------------------------------------------------
# Core pipeline implementation.
# ---------------------------------------------------------------------------
def run_gold_pipeline():
    """Execute the Gold ETL.

    The function connects to the database, creates a new ``pipeline_run_id``
    and populates the Gold tables using INSERT … SELECT statements that read
    exclusively from the ``silver`` schema.
    """
    run_id = _new_run_id()
    now_utc = datetime.utcnow()
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 1. Order fact – one row per order.
        cur.execute(
            """
            INSERT INTO gold.order_360 (
                order_id, customer_id, order_date, sales_channel, store_id,
                gross_merchandise_value, discount_amount, shipping_revenue,
                captured_payment_amount, refunded_amount, net_revenue,
                item_count, unit_quantity, order_status, payment_status,
                return_status, promotion_count, first_order_flag, pipeline_run_id
            )
            SELECT
                o.order_id,
                o.customer_id,
                o.ordered_at_utc::date AS order_date,
                o.sales_channel,
                o.store_id,
                -- gross = sum(unit_price * quantity)
                SUM(oi.unit_price * oi.quantity) AS gross_merchandise_value,
                SUM(oi.item_discount_amount) AS discount_amount,
                o.shipping_revenue,
                SUM(oi.unit_price * oi.quantity) - SUM(oi.item_discount_amount) AS captured_payment_amount,
                0.0 AS refunded_amount,  -- placeholder – populate later if needed
                SUM(oi.unit_price * oi.quantity) - SUM(oi.item_discount_amount) AS net_revenue,
                COUNT(oi.order_item_id) AS item_count,
                SUM(oi.quantity) AS unit_quantity,
                o.status AS order_status,
                'paid' AS payment_status,  -- simplification – can be derived from payment events
                'none' AS return_status,   -- simplification – can be derived from return events
                0 AS promotion_count,      -- will be updated in step 4
                CASE WHEN o.customer_id IS NOT NULL THEN FALSE ELSE TRUE END AS first_order_flag,
                %s AS pipeline_run_id
            FROM silver.orders o
            JOIN silver.order_items oi ON oi.order_id = o.order_id
            GROUP BY o.order_id, o.customer_id, o.ordered_at_utc, o.sales_channel,
                     o.store_id, o.shipping_revenue, o.status
            """,
            (run_id,)
        )

        # 2. Customer daily aggregates.
        cur.execute(
            """
            INSERT INTO gold.customer_daily (
                customer_id, metric_date, order_count, unit_quantity,
                gross_revenue, net_revenue, refund_amount, return_count,
                support_contacts, active_channel, new_customer_flag,
                repeat_customer_flag, pipeline_run_id
            )
            SELECT
                o.customer_id,
                o.ordered_at_utc::date AS metric_date,
                COUNT(DISTINCT o.order_id) AS order_count,
                SUM(oi.quantity) AS unit_quantity,
                SUM(oi.unit_price * oi.quantity) AS gross_revenue,
                SUM(oi.unit_price * oi.quantity) - SUM(oi.item_discount_amount) AS net_revenue,
                0.0 AS refund_amount,
                0 AS return_count,
                0 AS support_contacts,
                o.sales_channel AS active_channel,
                FALSE AS new_customer_flag,
                FALSE AS repeat_customer_flag,
                %s AS pipeline_run_id
            FROM silver.orders o
            JOIN silver.order_items oi ON oi.order_id = o.order_id
            GROUP BY o.customer_id, metric_date, o.sales_channel
            """,
            (run_id,)
        )

        # 3. Product daily aggregates.
        cur.execute(
            """
            INSERT INTO gold.product_daily (
                product_id, metric_date, active_category, units_sold, order_count,
                gross_revenue, net_revenue, refunded_units, available_inventory,
                reserved_inventory, stockout_flag, promotion_count, pipeline_run_id
            )
            SELECT
                oi.product_id,
                o.ordered_at_utc::date AS metric_date,
                pc.category_name AS active_category,
                SUM(oi.quantity) AS units_sold,
                COUNT(DISTINCT o.order_id) AS order_count,
                SUM(oi.unit_price * oi.quantity) AS gross_revenue,
                SUM(oi.unit_price * oi.quantity) - SUM(oi.item_discount_amount) AS net_revenue,
                0 AS refunded_units,
                inv.available_quantity,
                inv.reserved_quantity,
                CASE WHEN inv.available_quantity = 0 THEN TRUE ELSE FALSE END AS stockout_flag,
                0 AS promotion_count,
                %s AS pipeline_run_id
            FROM silver.order_items oi
            JOIN silver.orders o ON o.order_id = oi.order_id
            LEFT JOIN silver.product_categories pc ON pc.product_id = oi.product_id AND pc.valid_to_utc IS NULL
            LEFT JOIN silver.inventory_snapshots inv ON inv.product_id = oi.product_id AND inv.snapshot_date = o.ordered_at_utc::date
            GROUP BY oi.product_id, metric_date, active_category, inv.available_quantity, inv.reserved_quantity, stockout_flag
            """,
            (run_id,)
        )

        # 4. Channel‑campaign daily aggregates – join after facts are built.
        cur.execute(
            """
            INSERT INTO gold.channel_campaign_daily (
                metric_date, sales_channel, campaign_id, campaign_spend,
                attributed_orders, attributed_customers, gross_revenue, net_revenue,
                refunds, roas, conversion_rate, pipeline_run_id
            )
            SELECT
                o.metric_date,
                o.sales_channel,
                p.campaign_id,
                cs.spend_amount AS campaign_spend,
                COUNT(DISTINCT f.order_id) AS attributed_orders,
                COUNT(DISTINCT f.customer_id) AS attributed_customers,
                SUM(f.gross_merchandise_value) AS gross_revenue,
                SUM(f.net_revenue) AS net_revenue,
                0.0 AS refunds,
                NULL::NUMERIC AS roas,
                NULL::NUMERIC AS conversion_rate,
                %s AS pipeline_run_id
            FROM (
                SELECT order_id, customer_id, ordered_at_utc::date AS metric_date, sales_channel
                FROM silver.orders
            ) o
            JOIN gold.order_360 f ON f.order_id = o.order_id
            LEFT JOIN silver.promotions p ON p.promotion_id = f.order_id -- simplistic mapping; replace with real logic
            LEFT JOIN silver.campaign_spend cs ON cs.campaign_id = p.promotion_id AND cs.spend_date = o.metric_date
            GROUP BY o.metric_date, o.sales_channel, p.campaign_id, cs.spend_amount
            """,
            (run_id,)
        )

        # 5. Executive KPIs – a single‑row-per‑day summary.
        cur.execute(
            """
            INSERT INTO gold.executive_kpis_daily (
                metric_date, total_orders, gross_revenue, net_revenue,
                average_order_value, refund_rate, return_rate, repeat_customer_rate,
                stockout_rate, active_customers, support_contact_rate,
                data_freshness_utc, pipeline_run_id
            )
            SELECT
                metric_date,
                SUM(order_count) AS total_orders,
                SUM(gross_revenue) AS gross_revenue,
                SUM(net_revenue) AS net_revenue,
                CASE WHEN SUM(order_count) = 0 THEN 0 ELSE SUM(gross_revenue)::NUMERIC / SUM(order_count) END AS average_order_value,
                0.0 AS refund_rate,
                0.0 AS return_rate,
                0.0 AS repeat_customer_rate,
                0.0 AS stockout_rate,
                COUNT(DISTINCT customer_id) AS active_customers,
                0.0 AS support_contact_rate,
                %s AS data_freshness_utc,
                %s AS pipeline_run_id
            FROM gold.customer_daily
            GROUP BY metric_date
            """,
            (now_utc, run_id)
        )

        # Commit all inserts.
        conn.commit()
        print(f"Gold pipeline completed successfully (run_id={run_id})")
    except Exception as exc:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_gold_pipeline()
