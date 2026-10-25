# -*- coding: utf-8 -*-
"""Gold pipeline: materialize analytical Gold tables from Silver and validate Gold contracts.

Key design guarantees:
1. Reads exclusively from the ``silver`` schema (and already-materialized ``gold`` fact tables).
2. Aggregates child/event tables in isolated CTEs at the target grain BEFORE joining
   to dimensions, promotions, campaigns, or inventory snapshots — preventing fan-out
   and fact multiplication.
3. Computes all Gold metrics dynamically from Silver data (zero hardcoded placeholders):
   - ``gold.order_360``: one row per resolved order with GMV, item + promotion discounts,
     shipping revenue, captured payment amount, completed refund amount, net revenue
     (``GMV - discount_amount + shipping_revenue - refunded_amount``), item/unit counts,
     order/payment/return statuses, promotion count, and chronological ``first_order_flag``.
   - ``gold.customer_daily``: one row per ``(customer_id, metric_date)`` with order/unit counts,
     gross/net revenue, refund amount, return count, support contacts, dominant active channel,
     ``new_customer_flag``, and ``repeat_customer_flag``.
   - ``gold.product_daily``: one row per ``(product_id, metric_date)`` with point-in-time SCD2
     ``active_category``, units sold, order count, gross/net revenue, refunded units,
     pre-aggregated store inventory (``available_inventory``, ``reserved_inventory``,
     ``stockout_flag`` preserving NULL when snapshots are missing), and promotion count.
   - ``gold.channel_campaign_daily``: one row per ``(metric_date, sales_channel, campaign_id)``
     combining deduplicated web attribution and ``silver.campaign_spend`` via FULL OUTER JOIN
     so neither revenue nor spend is duplicated or lost, with accurate ``roas`` and ``conversion_rate``.
   - ``gold.executive_kpis_daily``: one row per ``metric_date`` summarizing daily orders,
     gross/net revenue, AOV, refund rate, return rate, repeat customer rate, stockout rate,
     active customers, support contact rate, and data freshness timestamp.
4. Supports idempotent re-runs via transactional rebuild + ``ON CONFLICT DO UPDATE`` and
   records lineage & load statistics in ``ops.layer_load_stats`` and ``ops.pipeline_runs``.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("LOCAL_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"),
)


def _new_run_id() -> str:
    """Generate a unique run identifier if none was supplied."""
    return f"gold_run_{uuid.uuid4().hex[:12]}"


def _get_connection():
    """Connect to PostgreSQL using psycopg2 (preferred) or pg8000 fallback."""
    try:
        import psycopg2

        conn = psycopg2.connect(DB_URL)
        conn.autocommit = False
        return conn, "psycopg2"
    except ImportError:
        try:
            from shared.db import connect

            conn = connect("local")
            return conn, "pg8000"
        except ImportError:
            from urllib.parse import unquote, urlsplit
            import pg8000.dbapi

            parsed = urlsplit(DB_URL)
            raw_conn = pg8000.dbapi.connect(
                user=unquote(parsed.username or "postgres"),
                password=unquote(parsed.password or "postgres"),
                host=parsed.hostname or "localhost",
                port=parsed.port or 5432,
                database=unquote(parsed.path.lstrip("/") or "postgres"),
            )
            return raw_conn, "pg8000_raw"


def _ensure_gold_schema(cur) -> None:
    """Ensure the ``gold`` schema and contract tables exist exactly as specified in ``gold.sql``."""
    cur.execute(
        """
        CREATE SCHEMA IF NOT EXISTS gold;

        CREATE TABLE IF NOT EXISTS gold.order_360 (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date DATE NOT NULL,
            sales_channel TEXT NOT NULL,
            store_id TEXT,
            gross_merchandise_value NUMERIC(14, 2) NOT NULL,
            discount_amount NUMERIC(14, 2) NOT NULL,
            shipping_revenue NUMERIC(14, 2) NOT NULL,
            captured_payment_amount NUMERIC(14, 2) NOT NULL,
            refunded_amount NUMERIC(14, 2) NOT NULL,
            net_revenue NUMERIC(14, 2) NOT NULL,
            item_count INTEGER NOT NULL,
            unit_quantity INTEGER NOT NULL,
            order_status TEXT NOT NULL,
            payment_status TEXT NOT NULL,
            return_status TEXT NOT NULL,
            promotion_count INTEGER NOT NULL,
            first_order_flag BOOLEAN NOT NULL,
            pipeline_run_id TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS gold.customer_daily (
            customer_id TEXT NOT NULL,
            metric_date DATE NOT NULL,
            order_count INTEGER NOT NULL,
            unit_quantity INTEGER NOT NULL,
            gross_revenue NUMERIC(14, 2) NOT NULL,
            net_revenue NUMERIC(14, 2) NOT NULL,
            refund_amount NUMERIC(14, 2) NOT NULL,
            return_count INTEGER NOT NULL,
            support_contacts INTEGER NOT NULL,
            active_channel TEXT,
            new_customer_flag BOOLEAN NOT NULL,
            repeat_customer_flag BOOLEAN NOT NULL,
            pipeline_run_id TEXT NOT NULL,
            PRIMARY KEY (customer_id, metric_date)
        );

        CREATE TABLE IF NOT EXISTS gold.product_daily (
            product_id TEXT NOT NULL,
            metric_date DATE NOT NULL,
            active_category TEXT,
            units_sold INTEGER NOT NULL,
            order_count INTEGER NOT NULL,
            gross_revenue NUMERIC(14, 2) NOT NULL,
            net_revenue NUMERIC(14, 2) NOT NULL,
            refunded_units INTEGER NOT NULL,
            available_inventory INTEGER,
            reserved_inventory INTEGER,
            stockout_flag BOOLEAN,
            promotion_count INTEGER NOT NULL,
            pipeline_run_id TEXT NOT NULL,
            PRIMARY KEY (product_id, metric_date)
        );

        CREATE TABLE IF NOT EXISTS gold.channel_campaign_daily (
            metric_date DATE NOT NULL,
            sales_channel TEXT NOT NULL,
            campaign_id TEXT NOT NULL,
            campaign_spend NUMERIC(14, 2) NOT NULL,
            attributed_orders INTEGER NOT NULL,
            attributed_customers INTEGER NOT NULL,
            gross_revenue NUMERIC(14, 2) NOT NULL,
            net_revenue NUMERIC(14, 2) NOT NULL,
            refunds NUMERIC(14, 2) NOT NULL,
            roas NUMERIC(14, 4),
            conversion_rate NUMERIC(14, 4),
            pipeline_run_id TEXT NOT NULL,
            PRIMARY KEY (metric_date, sales_channel, campaign_id)
        );

        CREATE TABLE IF NOT EXISTS gold.executive_kpis_daily (
            metric_date DATE PRIMARY KEY,
            total_orders INTEGER NOT NULL,
            gross_revenue NUMERIC(14, 2) NOT NULL,
            net_revenue NUMERIC(14, 2) NOT NULL,
            average_order_value NUMERIC(14, 2) NOT NULL,
            refund_rate NUMERIC(14, 4) NOT NULL,
            return_rate NUMERIC(14, 4) NOT NULL,
            repeat_customer_rate NUMERIC(14, 4) NOT NULL,
            stockout_rate NUMERIC(14, 4) NOT NULL,
            active_customers INTEGER NOT NULL,
            support_contact_rate NUMERIC(14, 4) NOT NULL,
            data_freshness_utc TIMESTAMPTZ NOT NULL,
            pipeline_run_id TEXT NOT NULL
        );
        """
    )


# ---------------------------------------------------------------------------
# Core Gold ETL
# ---------------------------------------------------------------------------
def run_gold(pipeline_run_id: str | None = None) -> str:
    """Materialize all five Gold tables from Silver and record ops lineage stats."""
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()

    try:
        _ensure_gold_schema(cur)

        if not pipeline_run_id:
            cur.execute(
                "SELECT pipeline_run_id FROM silver.orders ORDER BY ingested_at_utc DESC LIMIT 1"
            )
            row = cur.fetchone()
            pipeline_run_id = str(row[0]) if row else _new_run_id()

        run_id = pipeline_run_id
        now_utc = datetime.now(timezone.utc)

        # Clear Gold tables inside the transaction before rebuilding so reruns never leave stale rows
        cur.execute(
            """
            TRUNCATE TABLE
                gold.order_360,
                gold.customer_daily,
                gold.product_daily,
                gold.channel_campaign_daily,
                gold.executive_kpis_daily;
            """
        )

        # =====================================================================
        # 1. gold.order_360 (Grain: 1 row per resolved order)
        # =====================================================================
        cur.execute(
            """
            WITH item_agg AS (
                SELECT
                    oi.order_id,
                    COALESCE(SUM(oi.quantity * oi.unit_price), 0)::NUMERIC(14, 2) AS gross_merchandise_value,
                    COALESCE(SUM(oi.item_discount_amount), 0)::NUMERIC(14, 2) AS item_discount_amount,
                    COUNT(oi.order_item_id)::INTEGER AS item_count,
                    COALESCE(SUM(oi.quantity), 0)::INTEGER AS unit_quantity
                FROM silver.order_items oi
                GROUP BY oi.order_id
            ),
            promo_agg AS (
                SELECT
                    op.order_id,
                    COALESCE(SUM(op.discount_amount), 0)::NUMERIC(14, 2) AS promo_discount_amount,
                    COUNT(DISTINCT op.promotion_id)::INTEGER AS promotion_count
                FROM silver.order_promotions op
                GROUP BY op.order_id
            ),
            payment_agg AS (
                SELECT
                    pe.order_id,
                    COALESCE(
                        SUM(CASE WHEN pe.event_type = 'PAYMENT_CAPTURED' THEN pe.amount ELSE 0 END),
                        0
                    )::NUMERIC(14, 2) AS captured_payment_amount,
                    BOOL_OR(pe.event_type = 'PAYMENT_CAPTURED') AS has_captured,
                    BOOL_OR(pe.event_type = 'PAYMENT_AUTHORIZED') AS has_authorized,
                    BOOL_OR(pe.event_type = 'PAYMENT_FAILED') AS has_failed
                FROM silver.payment_events pe
                GROUP BY pe.order_id
            ),
            refund_dedup AS (
                SELECT DISTINCT ON (re.refund_id)
                    re.refund_id,
                    re.order_id,
                    re.amount
                FROM silver.refund_events re
                WHERE re.event_type = 'REFUND_COMPLETED'
                ORDER BY re.refund_id, re.occurred_at_utc DESC, re.event_id ASC
            ),
            refund_agg AS (
                SELECT
                    rd.order_id,
                    COALESCE(SUM(rd.amount), 0)::NUMERIC(14, 2) AS refunded_amount
                FROM refund_dedup rd
                GROUP BY rd.order_id
            ),
            return_agg AS (
                SELECT
                    rte.order_id,
                    BOOL_OR(rte.event_type = 'RETURN_CLOSED') AS has_closed,
                    BOOL_OR(rte.event_type = 'RETURN_RECEIVED') AS has_received,
                    BOOL_OR(rte.event_type = 'RETURN_REQUESTED') AS has_requested
                FROM silver.return_events rte
                GROUP BY rte.order_id
            ),
            order_seq AS (
                SELECT
                    o.order_id,
                    (
                        ROW_NUMBER() OVER (
                            PARTITION BY o.customer_id
                            ORDER BY o.ordered_at_utc ASC, o.order_id ASC
                        ) = 1
                    ) AS first_order_flag
                FROM silver.orders o
            )
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
                (o.ordered_at_utc AT TIME ZONE 'UTC')::date AS order_date,
                o.sales_channel,
                o.store_id,
                COALESCE(ia.gross_merchandise_value, 0)::NUMERIC(14, 2) AS gross_merchandise_value,
                (COALESCE(ia.item_discount_amount, 0) + COALESCE(pa.promo_discount_amount, 0))::NUMERIC(14, 2) AS discount_amount,
                COALESCE(o.shipping_revenue, 0)::NUMERIC(14, 2) AS shipping_revenue,
                COALESCE(pya.captured_payment_amount, 0)::NUMERIC(14, 2) AS captured_payment_amount,
                COALESCE(rfa.refunded_amount, 0)::NUMERIC(14, 2) AS refunded_amount,
                (
                    COALESCE(ia.gross_merchandise_value, 0)
                    - (COALESCE(ia.item_discount_amount, 0) + COALESCE(pa.promo_discount_amount, 0))
                    + COALESCE(o.shipping_revenue, 0)
                    - COALESCE(rfa.refunded_amount, 0)
                )::NUMERIC(14, 2) AS net_revenue,
                COALESCE(ia.item_count, 0)::INTEGER AS item_count,
                COALESCE(ia.unit_quantity, 0)::INTEGER AS unit_quantity,
                o.status::TEXT AS order_status,
                CASE
                    WHEN COALESCE(pya.has_captured, FALSE) THEN 'captured'
                    WHEN COALESCE(pya.has_failed, FALSE) THEN 'failed'
                    WHEN COALESCE(pya.has_authorized, FALSE) THEN 'authorized'
                    ELSE 'unpaid'
                END AS payment_status,
                CASE
                    WHEN COALESCE(rta.has_closed, FALSE) THEN 'closed'
                    WHEN COALESCE(rta.has_received, FALSE) THEN 'received'
                    WHEN COALESCE(rta.has_requested, FALSE) THEN 'requested'
                    WHEN o.status::TEXT = 'returned' THEN 'returned'
                    ELSE 'none'
                END AS return_status,
                COALESCE(pa.promotion_count, 0)::INTEGER AS promotion_count,
                COALESCE(oseq.first_order_flag, FALSE) AS first_order_flag,
                %s AS pipeline_run_id
            FROM silver.orders o
            LEFT JOIN item_agg ia ON ia.order_id = o.order_id
            LEFT JOIN promo_agg pa ON pa.order_id = o.order_id
            LEFT JOIN payment_agg pya ON pya.order_id = o.order_id
            LEFT JOIN refund_agg rfa ON rfa.order_id = o.order_id
            LEFT JOIN return_agg rta ON rta.order_id = o.order_id
            LEFT JOIN order_seq oseq ON oseq.order_id = o.order_id
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                order_date = EXCLUDED.order_date,
                sales_channel = EXCLUDED.sales_channel,
                store_id = EXCLUDED.store_id,
                gross_merchandise_value = EXCLUDED.gross_merchandise_value,
                discount_amount = EXCLUDED.discount_amount,
                shipping_revenue = EXCLUDED.shipping_revenue,
                captured_payment_amount = EXCLUDED.captured_payment_amount,
                refunded_amount = EXCLUDED.refunded_amount,
                net_revenue = EXCLUDED.net_revenue,
                item_count = EXCLUDED.item_count,
                unit_quantity = EXCLUDED.unit_quantity,
                order_status = EXCLUDED.order_status,
                payment_status = EXCLUDED.payment_status,
                return_status = EXCLUDED.return_status,
                promotion_count = EXCLUDED.promotion_count,
                first_order_flag = EXCLUDED.first_order_flag,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
            """,
            (run_id,),
        )

        # =====================================================================
        # 2. gold.customer_daily (Grain: 1 row per customer_id, metric_date)
        # =====================================================================
        cur.execute(
            """
            WITH support_per_order AS (
                SELECT
                    se.order_id,
                    COUNT(DISTINCT se.ticket_id)::INTEGER AS support_contacts
                FROM silver.support_events se
                WHERE se.order_id IS NOT NULL
                GROUP BY se.order_id
            )
            INSERT INTO gold.customer_daily (
                customer_id, metric_date, order_count, unit_quantity,
                gross_revenue, net_revenue, refund_amount, return_count,
                support_contacts, active_channel, new_customer_flag,
                repeat_customer_flag, pipeline_run_id
            )
            SELECT
                f.customer_id,
                f.order_date AS metric_date,
                COUNT(f.order_id)::INTEGER AS order_count,
                COALESCE(SUM(f.unit_quantity), 0)::INTEGER AS unit_quantity,
                COALESCE(SUM(f.gross_merchandise_value), 0)::NUMERIC(14, 2) AS gross_revenue,
                COALESCE(SUM(f.net_revenue), 0)::NUMERIC(14, 2) AS net_revenue,
                COALESCE(SUM(f.refunded_amount), 0)::NUMERIC(14, 2) AS refund_amount,
                COUNT(f.order_id) FILTER (WHERE f.return_status <> 'none')::INTEGER AS return_count,
                COALESCE(SUM(spo.support_contacts), 0)::INTEGER AS support_contacts,
                (ARRAY_AGG(f.sales_channel ORDER BY f.net_revenue DESC, f.order_id ASC))[1] AS active_channel,
                BOOL_OR(f.first_order_flag) AS new_customer_flag,
                BOOL_OR(NOT f.first_order_flag) AS repeat_customer_flag,
                %s AS pipeline_run_id
            FROM gold.order_360 f
            LEFT JOIN support_per_order spo ON spo.order_id = f.order_id
            GROUP BY f.customer_id, f.order_date
            ON CONFLICT (customer_id, metric_date) DO UPDATE SET
                order_count = EXCLUDED.order_count,
                unit_quantity = EXCLUDED.unit_quantity,
                gross_revenue = EXCLUDED.gross_revenue,
                net_revenue = EXCLUDED.net_revenue,
                refund_amount = EXCLUDED.refund_amount,
                return_count = EXCLUDED.return_count,
                support_contacts = EXCLUDED.support_contacts,
                active_channel = EXCLUDED.active_channel,
                new_customer_flag = EXCLUDED.new_customer_flag,
                repeat_customer_flag = EXCLUDED.repeat_customer_flag,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
            """,
            (run_id,),
        )

        # =====================================================================
        # 3. gold.product_daily (Grain: 1 row per product_id, metric_date)
        # =====================================================================
        cur.execute(
            """
            WITH order_promo_totals AS (
                SELECT
                    op.order_id,
                    COALESCE(SUM(op.discount_amount), 0)::NUMERIC(14, 2) AS promo_discount_amount
                FROM silver.order_promotions op
                GROUP BY op.order_id
            ),
            item_facts AS (
                SELECT
                    oi.product_id,
                    f.order_date AS metric_date,
                    oi.order_id,
                    oi.quantity,
                    (oi.quantity * oi.unit_price)::NUMERIC(14, 2) AS item_gross_revenue,
                    oi.item_discount_amount,
                    CASE
                        WHEN f.gross_merchandise_value > 0
                        THEN (oi.quantity * oi.unit_price)::NUMERIC / f.gross_merchandise_value
                        ELSE 0
                    END AS gmv_share,
                    COALESCE(opt.promo_discount_amount, 0) AS order_promo_discount,
                    f.refunded_amount AS order_refunded_amount,
                    f.gross_merchandise_value - f.discount_amount + f.shipping_revenue AS order_pre_refund_total,
                    f.return_status
                FROM silver.order_items oi
                JOIN gold.order_360 f ON f.order_id = oi.order_id
                LEFT JOIN order_promo_totals opt ON opt.order_id = oi.order_id
            ),
            product_sales_daily AS (
                SELECT
                    ifx.product_id,
                    ifx.metric_date,
                    SUM(ifx.quantity)::INTEGER AS units_sold,
                    COUNT(DISTINCT ifx.order_id)::INTEGER AS order_count,
                    ROUND(SUM(ifx.item_gross_revenue), 2)::NUMERIC(14, 2) AS gross_revenue,
                    ROUND(
                        SUM(
                            ifx.item_gross_revenue
                            - ifx.item_discount_amount
                            - (ifx.order_promo_discount * ifx.gmv_share)
                            - (ifx.order_refunded_amount * ifx.gmv_share)
                        ),
                        2
                    )::NUMERIC(14, 2) AS net_revenue,
                    SUM(
                        CASE
                            WHEN ifx.order_refunded_amount > 0 AND ifx.order_pre_refund_total > 0
                            THEN GREATEST(
                                1,
                                ROUND(
                                    ifx.quantity * LEAST(1.0, ifx.order_refunded_amount / ifx.order_pre_refund_total)
                                )::INTEGER
                            )
                            WHEN ifx.return_status <> 'none'
                            THEN ifx.quantity
                            ELSE 0
                        END
                    )::INTEGER AS refunded_units
                FROM item_facts ifx
                GROUP BY ifx.product_id, ifx.metric_date
            ),
            product_promos_daily AS (
                SELECT
                    oi.product_id,
                    f.order_date AS metric_date,
                    COUNT(DISTINCT op.promotion_id)::INTEGER AS promotion_count
                FROM silver.order_items oi
                JOIN gold.order_360 f ON f.order_id = oi.order_id
                JOIN silver.order_promotions op ON op.order_id = oi.order_id
                GROUP BY oi.product_id, f.order_date
            ),
            inventory_daily AS (
                SELECT
                    inv.product_id,
                    inv.snapshot_date AS metric_date,
                    SUM(inv.available_quantity)::INTEGER AS available_inventory,
                    SUM(inv.reserved_quantity)::INTEGER AS reserved_inventory,
                    BOOL_OR(inv.available_quantity <= 0) AS stockout_flag
                FROM silver.inventory_snapshots inv
                GROUP BY inv.product_id, inv.snapshot_date
            ),
            product_date_spine AS (
                SELECT product_id, metric_date FROM product_sales_daily
                UNION
                SELECT product_id, metric_date FROM inventory_daily
            ),
            product_category_pit AS (
                SELECT DISTINCT ON (pds.product_id, pds.metric_date)
                    pds.product_id,
                    pds.metric_date,
                    pc.category_name AS active_category
                FROM product_date_spine pds
                LEFT JOIN silver.product_categories pc
                    ON pc.product_id = pds.product_id
                   AND (pc.valid_from_utc AT TIME ZONE 'UTC')::date <= pds.metric_date
                   AND (pc.valid_to_utc IS NULL OR (pc.valid_to_utc AT TIME ZONE 'UTC')::date >= pds.metric_date)
                ORDER BY pds.product_id, pds.metric_date, pc.valid_from_utc DESC NULLS LAST
            )
            INSERT INTO gold.product_daily (
                product_id, metric_date, active_category, units_sold, order_count,
                gross_revenue, net_revenue, refunded_units, available_inventory,
                reserved_inventory, stockout_flag, promotion_count, pipeline_run_id
            )
            SELECT
                pds.product_id,
                pds.metric_date,
                pcp.active_category,
                COALESCE(psd.units_sold, 0)::INTEGER AS units_sold,
                COALESCE(psd.order_count, 0)::INTEGER AS order_count,
                COALESCE(psd.gross_revenue, 0.00)::NUMERIC(14, 2) AS gross_revenue,
                COALESCE(psd.net_revenue, 0.00)::NUMERIC(14, 2) AS net_revenue,
                COALESCE(psd.refunded_units, 0)::INTEGER AS refunded_units,
                invd.available_inventory,
                invd.reserved_inventory,
                invd.stockout_flag,
                COALESCE(ppd.promotion_count, 0)::INTEGER AS promotion_count,
                %s AS pipeline_run_id
            FROM product_date_spine pds
            LEFT JOIN product_sales_daily psd
                ON psd.product_id = pds.product_id AND psd.metric_date = pds.metric_date
            LEFT JOIN product_category_pit pcp
                ON pcp.product_id = pds.product_id AND pcp.metric_date = pds.metric_date
            LEFT JOIN inventory_daily invd
                ON invd.product_id = pds.product_id AND invd.metric_date = pds.metric_date
            LEFT JOIN product_promos_daily ppd
                ON ppd.product_id = pds.product_id AND ppd.metric_date = pds.metric_date
            ON CONFLICT (product_id, metric_date) DO UPDATE SET
                active_category = EXCLUDED.active_category,
                units_sold = EXCLUDED.units_sold,
                order_count = EXCLUDED.order_count,
                gross_revenue = EXCLUDED.gross_revenue,
                net_revenue = EXCLUDED.net_revenue,
                refunded_units = EXCLUDED.refunded_units,
                available_inventory = EXCLUDED.available_inventory,
                reserved_inventory = EXCLUDED.reserved_inventory,
                stockout_flag = EXCLUDED.stockout_flag,
                promotion_count = EXCLUDED.promotion_count,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
            """,
            (run_id,),
        )

        # =====================================================================
        # 4. gold.channel_campaign_daily (Grain: metric_date, sales_channel, campaign_id)
        # =====================================================================
        cur.execute(
            """
            WITH order_attribution AS (
                SELECT DISTINCT ON (we.order_id)
                    we.order_id,
                    COALESCE(NULLIF(TRIM(we.campaign_id), ''), 'UNATTRIBUTED') AS campaign_id
                FROM silver.web_events we
                WHERE we.order_id IS NOT NULL
                ORDER BY
                    we.order_id,
                    CASE WHEN we.campaign_id IS NOT NULL AND TRIM(we.campaign_id) <> '' THEN 0 ELSE 1 END,
                    CASE WHEN we.event_type = 'PURCHASE' THEN 0 ELSE 1 END,
                    we.occurred_at_utc ASC,
                    we.event_id ASC
            ),
            session_daily AS (
                SELECT
                    (we.occurred_at_utc AT TIME ZONE 'UTC')::date AS metric_date,
                    we.channel AS sales_channel,
                    COALESCE(NULLIF(TRIM(we.campaign_id), ''), 'UNATTRIBUTED') AS campaign_id,
                    COUNT(DISTINCT we.session_id)::INTEGER AS total_sessions
                FROM silver.web_events we
                WHERE we.channel IS NOT NULL
                GROUP BY 1, 2, 3
            ),
            attributed_orders_daily AS (
                SELECT
                    f.order_date AS metric_date,
                    f.sales_channel,
                    COALESCE(oa.campaign_id, 'UNATTRIBUTED') AS campaign_id,
                    COUNT(DISTINCT f.order_id)::INTEGER AS attributed_orders,
                    COUNT(DISTINCT f.customer_id)::INTEGER AS attributed_customers,
                    COALESCE(SUM(f.gross_merchandise_value), 0)::NUMERIC(14, 2) AS gross_revenue,
                    COALESCE(SUM(f.net_revenue), 0)::NUMERIC(14, 2) AS net_revenue,
                    COALESCE(SUM(f.refunded_amount), 0)::NUMERIC(14, 2) AS refunds,
                    COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status <> 'cancelled')::INTEGER AS converted_orders
                FROM gold.order_360 f
                LEFT JOIN order_attribution oa ON oa.order_id = f.order_id
                GROUP BY 1, 2, 3
            ),
            spend_daily AS (
                SELECT
                    cs.spend_date AS metric_date,
                    cs.channel AS sales_channel,
                    cs.campaign_id,
                    COALESCE(SUM(cs.spend_amount), 0)::NUMERIC(14, 2) AS campaign_spend
                FROM silver.campaign_spend cs
                GROUP BY 1, 2, 3
            )
            INSERT INTO gold.channel_campaign_daily (
                metric_date, sales_channel, campaign_id, campaign_spend,
                attributed_orders, attributed_customers, gross_revenue, net_revenue,
                refunds, roas, conversion_rate, pipeline_run_id
            )
            SELECT
                COALESCE(aod.metric_date, sd.metric_date) AS metric_date,
                COALESCE(aod.sales_channel, sd.sales_channel) AS sales_channel,
                COALESCE(aod.campaign_id, sd.campaign_id) AS campaign_id,
                COALESCE(sd.campaign_spend, 0.00)::NUMERIC(14, 2) AS campaign_spend,
                COALESCE(aod.attributed_orders, 0)::INTEGER AS attributed_orders,
                COALESCE(aod.attributed_customers, 0)::INTEGER AS attributed_customers,
                COALESCE(aod.gross_revenue, 0.00)::NUMERIC(14, 2) AS gross_revenue,
                COALESCE(aod.net_revenue, 0.00)::NUMERIC(14, 2) AS net_revenue,
                COALESCE(aod.refunds, 0.00)::NUMERIC(14, 2) AS refunds,
                CASE
                    WHEN COALESCE(sd.campaign_spend, 0) > 0
                    THEN ROUND(COALESCE(aod.net_revenue, 0) / sd.campaign_spend, 4)::NUMERIC(14, 4)
                    ELSE NULL
                END AS roas,
                CASE
                    WHEN COALESCE(sess.total_sessions, 0) > 0
                    THEN ROUND(COALESCE(aod.converted_orders, 0)::NUMERIC / sess.total_sessions, 4)::NUMERIC(14, 4)
                    WHEN COALESCE(aod.attributed_orders, 0) > 0
                    THEN ROUND(COALESCE(aod.converted_orders, 0)::NUMERIC / aod.attributed_orders, 4)::NUMERIC(14, 4)
                    ELSE 0.0000::NUMERIC(14, 4)
                END AS conversion_rate,
                %s AS pipeline_run_id
            FROM attributed_orders_daily aod
            FULL OUTER JOIN spend_daily sd
                ON sd.metric_date = aod.metric_date
               AND sd.sales_channel = aod.sales_channel
               AND sd.campaign_id = aod.campaign_id
            LEFT JOIN session_daily sess
                ON sess.metric_date = COALESCE(aod.metric_date, sd.metric_date)
               AND sess.sales_channel = COALESCE(aod.sales_channel, sd.sales_channel)
               AND sess.campaign_id = COALESCE(aod.campaign_id, sd.campaign_id)
            ON CONFLICT (metric_date, sales_channel, campaign_id) DO UPDATE SET
                campaign_spend = EXCLUDED.campaign_spend,
                attributed_orders = EXCLUDED.attributed_orders,
                attributed_customers = EXCLUDED.attributed_customers,
                gross_revenue = EXCLUDED.gross_revenue,
                net_revenue = EXCLUDED.net_revenue,
                refunds = EXCLUDED.refunds,
                roas = EXCLUDED.roas,
                conversion_rate = EXCLUDED.conversion_rate,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
            """,
            (run_id,),
        )

        # =====================================================================
        # 5. gold.executive_kpis_daily (Grain: 1 row per metric_date)
        # =====================================================================
        cur.execute(
            """
            WITH order_kpis AS (
                SELECT
                    f.order_date AS metric_date,
                    COUNT(f.order_id)::INTEGER AS total_orders,
                    COALESCE(SUM(f.gross_merchandise_value), 0)::NUMERIC(14, 2) AS gross_revenue,
                    COALESCE(SUM(f.net_revenue), 0)::NUMERIC(14, 2) AS net_revenue,
                    COALESCE(
                        ROUND(SUM(f.net_revenue) / NULLIF(COUNT(f.order_id), 0), 2),
                        0
                    )::NUMERIC(14, 2) AS average_order_value,
                    COALESCE(
                        ROUND(
                            COUNT(f.order_id) FILTER (WHERE f.refunded_amount > 0)::NUMERIC
                            / NULLIF(COUNT(f.order_id) FILTER (WHERE f.captured_payment_amount > 0), 0),
                            4
                        ),
                        0
                    )::NUMERIC(14, 4) AS refund_rate,
                    COALESCE(
                        ROUND(
                            COUNT(f.order_id) FILTER (WHERE f.return_status <> 'none')::NUMERIC
                            / NULLIF(COUNT(f.order_id), 0),
                            4
                        ),
                        0
                    )::NUMERIC(14, 4) AS return_rate
                FROM gold.order_360 f
                GROUP BY f.order_date
            ),
            customer_kpis AS (
                SELECT
                    cd.metric_date,
                    COUNT(DISTINCT cd.customer_id)::INTEGER AS active_customers,
                    COALESCE(
                        ROUND(AVG(CASE WHEN cd.repeat_customer_flag THEN 1.0 ELSE 0.0 END), 4),
                        0
                    )::NUMERIC(14, 4) AS repeat_customer_rate,
                    COALESCE(
                        ROUND(SUM(cd.support_contacts)::NUMERIC / NULLIF(SUM(cd.order_count), 0), 4),
                        0
                    )::NUMERIC(14, 4) AS support_contact_rate
                FROM gold.customer_daily cd
                GROUP BY cd.metric_date
            ),
            product_kpis AS (
                SELECT
                    pd.metric_date,
                    COALESCE(
                        ROUND(AVG(pd.stockout_flag::INT), 4),
                        0
                    )::NUMERIC(14, 4) AS stockout_rate
                FROM gold.product_daily pd
                WHERE pd.stockout_flag IS NOT NULL
                GROUP BY pd.metric_date
            )
            INSERT INTO gold.executive_kpis_daily (
                metric_date, total_orders, gross_revenue, net_revenue,
                average_order_value, refund_rate, return_rate, repeat_customer_rate,
                stockout_rate, active_customers, support_contact_rate,
                data_freshness_utc, pipeline_run_id
            )
            SELECT
                ok.metric_date,
                ok.total_orders,
                ok.gross_revenue,
                ok.net_revenue,
                ok.average_order_value,
                ok.refund_rate,
                ok.return_rate,
                COALESCE(ck.repeat_customer_rate, 0.0000)::NUMERIC(14, 4) AS repeat_customer_rate,
                COALESCE(pk.stockout_rate, 0.0000)::NUMERIC(14, 4) AS stockout_rate,
                COALESCE(ck.active_customers, 0)::INTEGER AS active_customers,
                COALESCE(ck.support_contact_rate, 0.0000)::NUMERIC(14, 4) AS support_contact_rate,
                %s AS data_freshness_utc,
                %s AS pipeline_run_id
            FROM order_kpis ok
            LEFT JOIN customer_kpis ck ON ck.metric_date = ok.metric_date
            LEFT JOIN product_kpis pk ON pk.metric_date = ok.metric_date
            ON CONFLICT (metric_date) DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                gross_revenue = EXCLUDED.gross_revenue,
                net_revenue = EXCLUDED.net_revenue,
                average_order_value = EXCLUDED.average_order_value,
                refund_rate = EXCLUDED.refund_rate,
                return_rate = EXCLUDED.return_rate,
                repeat_customer_rate = EXCLUDED.repeat_customer_rate,
                stockout_rate = EXCLUDED.stockout_rate,
                active_customers = EXCLUDED.active_customers,
                support_contact_rate = EXCLUDED.support_contact_rate,
                data_freshness_utc = EXCLUDED.data_freshness_utc,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
            """,
            (now_utc, run_id),
        )

        # =====================================================================
        # Record Gold load statistics into ops.layer_load_stats & ops.pipeline_runs
        # =====================================================================
        gold_tables = [
            "gold.order_360",
            "gold.customer_daily",
            "gold.product_daily",
            "gold.channel_campaign_daily",
            "gold.executive_kpis_daily",
        ]
        table_counts: dict[str, int] = {}
        for tbl in gold_tables:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            cnt = int(cur.fetchone()[0])
            table_counts[tbl] = cnt
            print(f"[Gold] {tbl}: {cnt} rows materialized")

        total_gold_rows = sum(table_counts.values())

        cur.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, status, gold_record_count)
            VALUES (%s, 'RUNNING', %s)
            ON CONFLICT (run_id) DO UPDATE SET
                gold_record_count = EXCLUDED.gold_record_count;
            """,
            (run_id, total_gold_rows),
        )

        cur.execute(
            "DELETE FROM ops.layer_load_stats WHERE pipeline_run_id = %s AND layer = 'gold'",
            (run_id,),
        )
        for tbl, cnt in table_counts.items():
            cur.execute(
                """
                INSERT INTO ops.layer_load_stats (
                    pipeline_run_id, layer, table_name, records_loaded, records_rejected
                )
                VALUES (%s, 'gold', %s, %s, 0)
                """,
                (run_id, tbl, cnt),
            )

        conn.commit()
        print(f"Gold pipeline completed successfully (run_id={run_id}, total_rows={total_gold_rows})")
        return run_id
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def run_gold_pipeline(pipeline_run_id: str | None = None) -> str:
    """Backward-compatible entrypoint alias for ``run_gold``."""
    return run_gold(pipeline_run_id=pipeline_run_id)


# ---------------------------------------------------------------------------
# Gold Validation & Cross-Table Reconciliation
# ---------------------------------------------------------------------------
def validate_gold(pipeline_run_id: str | None = None) -> bool:
    """Validate Gold tables for grain uniqueness, business formulas, and Silver-to-Gold reconciliation."""
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()

    try:
        if not pipeline_run_id:
            cur.execute("SELECT pipeline_run_id FROM gold.order_360 LIMIT 1")
            row = cur.fetchone()
            pipeline_run_id = str(row[0]) if row else _new_run_id()

        cur.execute(
            """
            DELETE FROM ops.quality_check_results
            WHERE pipeline_run_id = %s AND target_table LIKE 'gold.%%'
            """,
            (pipeline_run_id,),
        )

        checks: list[tuple[str, str, str, str, str, Any]] = [
            (
                "gold_all_tables_populated",
                "business_rule",
                "gold.order_360",
                """
                SELECT LEAST(
                    (SELECT COUNT(*) FROM gold.order_360),
                    (SELECT COUNT(*) FROM gold.customer_daily),
                    (SELECT COUNT(*) FROM gold.product_daily),
                    (SELECT COUNT(*) FROM gold.channel_campaign_daily),
                    (SELECT COUNT(*) FROM gold.executive_kpis_daily)
                )
                """,
                "> 0",
                lambda val: int(val) > 0,
            ),
            (
                "gold_order_360_grain_matches_silver_orders",
                "reconciliation",
                "gold.order_360",
                """
                SELECT ABS(
                    (SELECT COUNT(*) FROM gold.order_360) -
                    (SELECT COUNT(*) FROM silver.orders)
                )
                """,
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "gold_order_360_net_revenue_identity",
                "business_rule",
                "gold.order_360",
                """
                SELECT COUNT(*)
                FROM gold.order_360
                WHERE net_revenue <> (gross_merchandise_value - discount_amount + shipping_revenue - refunded_amount)
                """,
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "gold_order_360_payment_reconciliation",
                "reconciliation",
                "gold.order_360",
                """
                SELECT ABS(
                    (SELECT COALESCE(SUM(captured_payment_amount), 0) FROM gold.order_360) -
                    (SELECT COALESCE(SUM(amount), 0) FROM silver.payment_events WHERE event_type = 'PAYMENT_CAPTURED')
                )
                """,
                "0.00",
                lambda val: float(val) == 0.0,
            ),
            (
                "gold_order_360_refund_reconciliation",
                "reconciliation",
                "gold.order_360",
                """
                SELECT ABS(
                    (SELECT COALESCE(SUM(refunded_amount), 0) FROM gold.order_360) -
                    (SELECT COALESCE(SUM(amount), 0) FROM (
                        SELECT DISTINCT ON (refund_id) refund_id, amount
                        FROM silver.refund_events
                        WHERE event_type = 'REFUND_COMPLETED'
                        ORDER BY refund_id, occurred_at_utc DESC, event_id ASC
                    ) r)
                )
                """,
                "0.00",
                lambda val: float(val) == 0.0,
            ),
            (
                "gold_customer_daily_revenue_and_order_reconciliation",
                "reconciliation",
                "gold.customer_daily",
                """
                SELECT (
                    ABS((SELECT COALESCE(SUM(order_count), 0) FROM gold.customer_daily) - (SELECT COUNT(*) FROM gold.order_360)) +
                    ABS((SELECT COALESCE(SUM(gross_revenue), 0) FROM gold.customer_daily) - (SELECT COALESCE(SUM(gross_merchandise_value), 0) FROM gold.order_360)) +
                    ABS((SELECT COALESCE(SUM(net_revenue), 0) FROM gold.customer_daily) - (SELECT COALESCE(SUM(net_revenue), 0) FROM gold.order_360)) +
                    ABS((SELECT COALESCE(SUM(refund_amount), 0) FROM gold.customer_daily) - (SELECT COALESCE(SUM(refunded_amount), 0) FROM gold.order_360))
                )
                """,
                "0.00",
                lambda val: float(val) == 0.0,
            ),
            (
                "gold_product_daily_gmv_and_units_reconciliation",
                "reconciliation",
                "gold.product_daily",
                """
                SELECT (
                    ABS((SELECT COALESCE(SUM(units_sold), 0) FROM gold.product_daily) - (SELECT COALESCE(SUM(unit_quantity), 0) FROM gold.order_360)) +
                    ABS((SELECT COALESCE(SUM(gross_revenue), 0) FROM gold.product_daily) - (SELECT COALESCE(SUM(gross_merchandise_value), 0) FROM gold.order_360))
                )
                """,
                "0.00",
                lambda val: float(val) == 0.0,
            ),
            (
                "gold_channel_campaign_daily_spend_and_revenue_reconciliation",
                "reconciliation",
                "gold.channel_campaign_daily",
                """
                SELECT (
                    ABS((SELECT COALESCE(SUM(campaign_spend), 0) FROM gold.channel_campaign_daily) - (SELECT COALESCE(SUM(spend_amount), 0) FROM silver.campaign_spend)) +
                    ABS((SELECT COALESCE(SUM(attributed_orders), 0) FROM gold.channel_campaign_daily) - (SELECT COUNT(*) FROM gold.order_360)) +
                    ABS((SELECT COALESCE(SUM(net_revenue), 0) FROM gold.channel_campaign_daily) - (SELECT COALESCE(SUM(net_revenue), 0) FROM gold.order_360)) +
                    ABS((SELECT COALESCE(SUM(refunds), 0) FROM gold.channel_campaign_daily) - (SELECT COALESCE(SUM(refunded_amount), 0) FROM gold.order_360))
                )
                """,
                "0.00",
                lambda val: float(val) == 0.0,
            ),
            (
                "gold_executive_kpis_daily_reconciliation",
                "reconciliation",
                "gold.executive_kpis_daily",
                """
                SELECT (
                    ABS((SELECT COALESCE(SUM(total_orders), 0) FROM gold.executive_kpis_daily) - (SELECT COUNT(*) FROM gold.order_360)) +
                    ABS((SELECT COALESCE(SUM(gross_revenue), 0) FROM gold.executive_kpis_daily) - (SELECT COALESCE(SUM(gross_merchandise_value), 0) FROM gold.order_360)) +
                    ABS((SELECT COALESCE(SUM(net_revenue), 0) FROM gold.executive_kpis_daily) - (SELECT COALESCE(SUM(net_revenue), 0) FROM gold.order_360))
                )
                """,
                "0.00",
                lambda val: float(val) == 0.0,
            ),
            (
                "gold_executive_kpis_rates_valid_bounds",
                "business_rule",
                "gold.executive_kpis_daily",
                """
                SELECT COUNT(*)
                FROM gold.executive_kpis_daily
                WHERE refund_rate < 0 OR refund_rate > 1
                   OR return_rate < 0 OR return_rate > 1
                   OR repeat_customer_rate < 0 OR repeat_customer_rate > 1
                   OR stockout_rate < 0 OR stockout_rate > 1
                   OR support_contact_rate < 0 OR support_contact_rate > 1
                """,
                "0",
                lambda val: int(val) == 0,
            ),
        ]

        all_passed = True
        for check_name, category, target_table, sql_query, expected_str, predicate in checks:
            cur.execute(sql_query)
            actual_val = cur.fetchone()[0]
            passed = bool(predicate(actual_val))
            if not passed:
                all_passed = False

            details_json = json.dumps(
                {
                    "layer": "gold",
                    "expected": expected_str,
                    "actual": str(actual_val),
                }
            )
            cur.execute(
                """
                INSERT INTO ops.quality_check_results (
                    pipeline_run_id, check_name, check_category, target_table,
                    check_query, expected_result, actual_result, passed, details
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CAST(%s AS JSONB))
                """,
                (
                    pipeline_run_id,
                    check_name,
                    category,
                    target_table,
                    " ".join(sql_query.split()),
                    expected_str,
                    str(actual_val),
                    passed,
                    details_json,
                ),
            )
            status_label = "PASS" if passed else "FAIL"
            print(f"[ValidateGold] {check_name} ({target_table}): {status_label} (actual={actual_val}, expected={expected_str})")

        if not all_passed:
            conn.commit()
            raise RuntimeError("Gold validation failed one or more checks; see ops.quality_check_results")

        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    rid = run_gold()
    validate_gold(rid)
