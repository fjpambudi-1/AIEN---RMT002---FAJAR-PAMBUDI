# -*- coding: utf-8 -*-
"""Silver pipeline: transform raw Bronze JSONB records into typed, normalized Silver tables.

Responsibilities according to README:
1. Timestamp normalization to UTC (handling mixed timezone offsets like +07:00 and Z).
2. Deterministic deduplication using business primary keys and ``source_row_id``.
3. Foreign-key validation against parent Silver tables.
4. Status normalization (mapping raw order statuses to normalized Silver enum values).
5. Explicit rejection of negative quantities, invalid amounts, duplicate keys, and
   missing foreign references into ``silver.rejected_records`` with clear reasons.
6. Event identity resolution across ``payment_events``, ``refund_events``,
   ``return_events``, ``support_events``, and ``web_events`` (including
   ``silver.event_identity``).
7. Temporal version selection (SCD2 interval normalization for
   ``customer_profiles``, ``customer_addresses``, and ``product_categories``).
8. Lineage preservation (``bronze_id``, ``source_row_id``, ``pipeline_run_id``).
9. Late-arriving record handling (ordering by ``occurred_at_utc`` vs ``ingested_at_utc``
   and idempotent upserts).
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("LOCAL_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"),
)

# Normalized mapping for raw order statuses -> Silver order_status enum values
ORDER_STATUS_MAP: dict[str, str] = {
    "PLACED": "pending",
    "PENDING": "pending",
    "NEW": "pending",
    "CREATED": "pending",
    "CONFIRMED": "processing",
    "PROCESSING": "processing",
    "IN_PROGRESS": "processing",
    "FULFILLED": "shipped",
    "SHIPPED": "shipped",
    "DELIVERED": "shipped",
    "COMPLETED": "shipped",
    "CANCELLED": "cancelled",
    "CANCELED": "cancelled",
    "RETURNED": "returned",
}

# Dataset horizon used by the synthetic generator for open-ended SCD2 rows
OPEN_ENDED_SENTINEL_UTC = datetime(2026, 9, 21, 0, 0, 0, tzinfo=timezone.utc)


def _new_run_id() -> str:
    """Generate a unique run identifier if none was supplied."""
    return f"silver_run_{uuid.uuid4().hex[:12]}"


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


# ---------------------------------------------------------------------------
# Parsing & Normalization Helpers
# ---------------------------------------------------------------------------
def _parse_payload(raw_payload: Any) -> dict[str, Any]:
    """Ensure raw_payload from Bronze is a Python dict."""
    if isinstance(raw_payload, dict):
        return raw_payload
    if isinstance(raw_payload, str):
        return json.loads(raw_payload)
    return dict(raw_payload)


def _parse_utc_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp with any timezone offset and normalize to UTC."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_date(value: Any) -> date | None:
    """Parse an ISO-8601 date or timestamp string into a ``date`` object."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if "T" in text:
        ts = _parse_utc_timestamp(text)
        return ts.date() if ts else None
    return date.fromisoformat(text[:10])


def _parse_decimal(value: Any, default: Decimal | None = None) -> Decimal | None:
    """Parse a numeric value into a Decimal."""
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def _parse_int(value: Any) -> int | None:
    """Parse an integer value safely."""
    if value is None or value == "":
        return None
    try:
        return int(Decimal(str(value).strip()))
    except (InvalidOperation, ValueError):
        return None


def _clean_source_row_id(source_row_id: str | None) -> str:
    """Strip any Bronze duplicate-row suffix (``#rowN``) for canonical comparison."""
    if not source_row_id:
        return ""
    return str(source_row_id).split("#row")[0]


def _is_explicit_duplicate_row_id(source_row_id: str | None) -> bool:
    """Detect synthetic duplicate markers so canonical rows win deterministically."""
    if not source_row_id:
        return True
    s = str(source_row_id).upper()
    return "DUPLICATE" in s or "#ROW" in s


def _normalize_order_status(raw_status: Any) -> str | None:
    """Normalize raw order status strings to Silver enum values."""
    if raw_status is None:
        return None
    key = str(raw_status).strip().upper()
    return ORDER_STATUS_MAP.get(key)


# ---------------------------------------------------------------------------
# Bronze Reader & Rejection Writer
# ---------------------------------------------------------------------------
def _fetch_bronze_rows(
    cur,
    bronze_table: str,
    pipeline_run_id: str | None,
) -> list[tuple[int, str, dict[str, Any], datetime]]:
    """Fetch ``(bronze_id, source_row_id, payload_dict, ingested_at)`` from Bronze.

    Reads rows matching ``pipeline_run_id`` if present; otherwise reads the
    most recent ingestion batch from ``bronze_table``.
    """
    if pipeline_run_id:
        cur.execute(
            f"""
            SELECT bronze_id, source_row_id, raw_payload, ingested_at
            FROM {bronze_table}
            WHERE pipeline_run_id = %s
            ORDER BY source_row_num ASC NULLS LAST, bronze_id ASC
            """,
            (pipeline_run_id,),
        )
        rows = cur.fetchall()
        if rows:
            return [
                (int(r[0]), str(r[1] or ""), _parse_payload(r[2]), _parse_utc_timestamp(r[3]))
                for r in rows
            ]

    # Fallback: read latest run in the Bronze table
    cur.execute(
        f"""
        SELECT bronze_id, source_row_id, raw_payload, ingested_at
        FROM {bronze_table}
        WHERE pipeline_run_id = (
            SELECT pipeline_run_id
            FROM {bronze_table}
            ORDER BY ingested_at DESC, bronze_id DESC
            LIMIT 1
        )
        ORDER BY source_row_num ASC NULLS LAST, bronze_id ASC
        """
    )
    rows = cur.fetchall()
    return [
        (int(r[0]), str(r[1] or ""), _parse_payload(r[2]), _parse_utc_timestamp(r[3]))
        for r in rows
    ]


class _RejectionCollector:
    """Collect rejected Bronze records and persist them into ``silver.rejected_records``."""

    def __init__(self, pipeline_run_id: str) -> None:
        self.pipeline_run_id = pipeline_run_id
        self.records: list[dict[str, Any]] = []
        self.per_table_counts: dict[str, int] = {}

    def add(
        self,
        source_table: str,
        source_row_id: str | None,
        bronze_id: int | None,
        reason: str,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        self.records.append(
            {
                "source_table": source_table,
                "source_row_id": source_row_id or "UNKNOWN",
                "bronze_id": bronze_id,
                "rejection_reason": reason,
                "raw_payload": json.dumps(raw_payload or {}, ensure_ascii=False),
                "pipeline_run_id": self.pipeline_run_id,
            }
        )
        self.per_table_counts[source_table] = self.per_table_counts.get(source_table, 0) + 1

    def flush(self, cur, driver: str) -> int:
        if not self.records:
            return 0

        # Inspect available columns on silver.rejected_records to support both schema variants
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'silver' AND table_name = 'rejected_records'
            """
        )
        cols = {r[0] for r in cur.fetchall()}

        # Clear prior rejections so re-runs (even with a new run_id) do not duplicate rejected rows
        cur.execute("DELETE FROM silver.rejected_records")

        if "rejection_reason" in cols and "source_table" in cols:
            for rec in self.records:
                cur.execute(
                    """
                    INSERT INTO silver.rejected_records (
                        source_table, source_row_id, bronze_id,
                        rejection_reason, raw_payload, pipeline_run_id
                    )
                    VALUES (%s, %s, %s, %s, CAST(%s AS JSONB), %s)
                    """,
                    (
                        rec["source_table"],
                        rec["source_row_id"],
                        rec["bronze_id"],
                        rec["rejection_reason"],
                        rec["raw_payload"],
                        rec["pipeline_run_id"],
                    ),
                )
        else:
            for rec in self.records:
                cur.execute(
                    """
                    INSERT INTO silver.rejected_records (
                        source_row_id, bronze_id, pipeline_run_id, reason
                    )
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        rec["source_row_id"],
                        rec["bronze_id"],
                        rec["pipeline_run_id"],
                        f"{rec['source_table']}: {rec['rejection_reason']}",
                    ),
                )
        return len(self.records)


def _exec_batch(cur, driver: str, sql_values: str, sql_row: str, rows: list[tuple[Any, ...]], template: str | None = None) -> None:
    """Execute batch upserts using execute_values on psycopg2 or executemany on pg8000."""
    if not rows:
        return
    if driver == "psycopg2":
        from psycopg2.extras import execute_values

        execute_values(cur, sql_values, rows, template=template, page_size=2000)
    else:
        cur.executemany(sql_row, rows)


# ---------------------------------------------------------------------------
# SCD2 Temporal Version Normalization Helper
# ---------------------------------------------------------------------------
def _normalize_scd2_versions(
    grouped_rows: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Order SCD2 rows per entity by ``valid_from_utc`` and normalize open-ended ``valid_to_utc``.

    For each entity key (e.g. ``customer_id`` or ``product_id``):
    - Sorts versions by ``(valid_from_utc ASC, source_row_id ASC, bronze_id ASC)``.
    - Ensures contiguous non-overlapping intervals.
    - Sets ``valid_to_utc = None`` on the latest active version when its raw
      ``valid_to_utc`` is at or beyond the open-ended dataset horizon
      (``2026-09-21T00:00:00Z``), so both ``valid_to_utc IS NULL`` current-version
      joins and point-in-time range joins work accurately.
    """
    normalized: list[dict[str, Any]] = []
    for _entity_id, versions in grouped_rows.items():
        versions.sort(
            key=lambda r: (
                r["valid_from_utc"],
                _is_explicit_duplicate_row_id(r["source_row_id"]),
                r["source_row_id"],
                r["bronze_id"],
            )
        )
        for i, ver in enumerate(versions):
            is_last = i == len(versions) - 1
            valid_to = ver["valid_to_utc"]
            if is_last and (valid_to is None or valid_to >= OPEN_ENDED_SENTINEL_UTC):
                ver["valid_to_utc"] = None
            elif not is_last and valid_to is None:
                ver["valid_to_utc"] = versions[i + 1]["valid_from_utc"]
            normalized.append(ver)
    return normalized


# ---------------------------------------------------------------------------
# Main Silver Transformation
# ---------------------------------------------------------------------------
def run_silver(pipeline_run_id: str | None = None) -> str:
    """Transform Bronze -> Silver.

    Extracts data from the Bronze tables and performs:
    * deterministic deduplication (using ``source_row_id`` and business keys)
    * foreign-key validation against parent Silver tables
    * timestamp normalization to UTC
    * status normalization and SCD2 temporal version resolution
    * event identity resolution and late-arriving record handling
    * rejection handling into ``silver.rejected_records`` with a reason
    """
    run_id = pipeline_run_id or _new_run_id()
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()

    rejections = _RejectionCollector(run_id)
    loaded_stats: dict[str, int] = {}

    try:
        # Ensure bronze.raw_ingestion exists and is synced if Silver FKs reference it
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze.raw_ingestion (
                id              BIGINT PRIMARY KEY,
                source_table    TEXT,
                source_file     TEXT,
                source_row_id   TEXT,
                pipeline_run_id TEXT,
                ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        for b_tbl in [
            "bronze.raw_city_reference",
            "bronze.raw_customers",
            "bronze.raw_customer_profiles",
            "bronze.raw_customer_addresses",
            "bronze.raw_products",
            "bronze.raw_product_categories",
            "bronze.raw_stores",
            "bronze.raw_sales_channels",
            "bronze.raw_promotions",
            "bronze.raw_orders",
            "bronze.raw_order_items",
            "bronze.raw_order_promotions",
            "bronze.raw_payment_events",
            "bronze.raw_refund_events",
            "bronze.raw_return_events",
            "bronze.raw_support_events",
            "bronze.raw_web_events",
            "bronze.raw_inventory_snapshots",
            "bronze.raw_campaign_spend",
        ]:
            cur.execute(
                f"""
                INSERT INTO bronze.raw_ingestion (id, source_table, source_file, source_row_id, pipeline_run_id, ingested_at)
                SELECT bronze_id, %s, source_file, source_row_id, pipeline_run_id, ingested_at
                FROM {b_tbl}
                ON CONFLICT (id) DO NOTHING;
                """,
                (b_tbl,),
            )

        # =====================================================================
        # 1. silver.city_reference
        # =====================================================================
        raw_cities = _fetch_bronze_rows(cur, "bronze.raw_city_reference", pipeline_run_id)
        cities_by_id: dict[str, tuple[Any, ...]] = {}
        for bronze_id, s_row_id, payload, _ing_at in raw_cities:
            city_id = str(payload.get("city_id") or "").strip().upper()
            city_name = str(payload.get("city_name") or "").strip() or None
            country = str(payload.get("country") or "").strip().upper() or None
            if not city_id:
                rejections.add("silver.city_reference", s_row_id, bronze_id, "missing_city_id", payload)
                continue
            if city_id in cities_by_id:
                rejections.add("silver.city_reference", s_row_id, bronze_id, "duplicate_city_id", payload)
                continue
            cities_by_id[city_id] = (city_id, city_name, country, bronze_id, run_id)

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.city_reference (city_id, city_name, country, bronze_id, pipeline_run_id)
            VALUES %s
            ON CONFLICT (city_id) DO UPDATE SET
                city_name = EXCLUDED.city_name,
                country = EXCLUDED.country,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.city_reference (city_id, city_name, country, bronze_id, pipeline_run_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (city_id) DO UPDATE SET
                city_name = EXCLUDED.city_name,
                country = EXCLUDED.country,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(cities_by_id.values()),
        )
        valid_city_ids = set(cities_by_id.keys())
        loaded_stats["silver.city_reference"] = len(cities_by_id)

        # =====================================================================
        # 2. silver.sales_channels
        # =====================================================================
        raw_channels = _fetch_bronze_rows(cur, "bronze.raw_sales_channels", pipeline_run_id)
        channels_by_id: dict[str, tuple[Any, ...]] = {}
        seen_channel_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_channels:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            channel_id = str(payload.get("channel_id") or "").strip().upper()
            channel_name = str(payload.get("channel_name") or channel_id).strip().upper()
            if not channel_id or not clean_sid:
                rejections.add("silver.sales_channels", s_row_id, bronze_id, "missing_channel_id", payload)
                continue
            if channel_id in channels_by_id or clean_sid in seen_channel_sids:
                rejections.add("silver.sales_channels", s_row_id, bronze_id, "duplicate_channel", payload)
                continue
            seen_channel_sids.add(clean_sid)
            channels_by_id[channel_id] = (channel_id, channel_name, clean_sid, bronze_id, run_id)

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.sales_channels (channel_id, channel_name, source_row_id, bronze_id, pipeline_run_id)
            VALUES %s
            ON CONFLICT (channel_id) DO UPDATE SET
                channel_name = EXCLUDED.channel_name,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.sales_channels (channel_id, channel_name, source_row_id, bronze_id, pipeline_run_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (channel_id) DO UPDATE SET
                channel_name = EXCLUDED.channel_name,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(channels_by_id.values()),
        )
        valid_channel_ids = set(channels_by_id.keys())
        loaded_stats["silver.sales_channels"] = len(channels_by_id)

        # =====================================================================
        # 3. silver.stores
        # =====================================================================
        raw_stores = _fetch_bronze_rows(cur, "bronze.raw_stores", pipeline_run_id)
        stores_by_id: dict[str, tuple[Any, ...]] = {}
        seen_store_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_stores:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            store_id = str(payload.get("store_id") or "").strip()
            store_name = str(payload.get("store_name") or "").strip() or None
            city_id = str(payload.get("city_id") or "").strip().upper() or None
            loc_type = str(payload.get("location_type") or "").strip().lower() or None

            if not store_id or not clean_sid:
                rejections.add("silver.stores", s_row_id, bronze_id, "missing_store_id", payload)
                continue
            if city_id and valid_city_ids and city_id not in valid_city_ids:
                rejections.add("silver.stores", s_row_id, bronze_id, "missing_fk_city_id", payload)
                continue
            if store_id in stores_by_id or clean_sid in seen_store_sids:
                rejections.add("silver.stores", s_row_id, bronze_id, "duplicate_store", payload)
                continue
            seen_store_sids.add(clean_sid)
            stores_by_id[store_id] = (store_id, store_name, city_id, loc_type, clean_sid, bronze_id, run_id)

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.stores (store_id, store_name, city_id, location_type, source_row_id, bronze_id, pipeline_run_id)
            VALUES %s
            ON CONFLICT (store_id) DO UPDATE SET
                store_name = EXCLUDED.store_name,
                city_id = EXCLUDED.city_id,
                location_type = EXCLUDED.location_type,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.stores (store_id, store_name, city_id, location_type, source_row_id, bronze_id, pipeline_run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_id) DO UPDATE SET
                store_name = EXCLUDED.store_name,
                city_id = EXCLUDED.city_id,
                location_type = EXCLUDED.location_type,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(stores_by_id.values()),
        )
        valid_store_ids = set(stores_by_id.keys())
        loaded_stats["silver.stores"] = len(stores_by_id)

        # =====================================================================
        # 4. silver.customers
        # =====================================================================
        raw_customers = _fetch_bronze_rows(cur, "bronze.raw_customers", pipeline_run_id)
        customers_by_id: dict[str, tuple[Any, ...]] = {}
        seen_cust_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_customers:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            customer_id = str(payload.get("customer_id") or "").strip()
            city_id = str(payload.get("city_id") or "").strip().upper() or None
            segment = str(payload.get("customer_segment") or "").strip().lower() or None
            created_at = _parse_utc_timestamp(payload.get("created_at_utc"))

            if not customer_id or not clean_sid or created_at is None:
                rejections.add("silver.customers", s_row_id, bronze_id, "missing_required_customer_fields", payload)
                continue
            if city_id and valid_city_ids and city_id not in valid_city_ids:
                rejections.add("silver.customers", s_row_id, bronze_id, "missing_fk_city_id", payload)
                continue
            if customer_id in customers_by_id or clean_sid in seen_cust_sids:
                rejections.add("silver.customers", s_row_id, bronze_id, "duplicate_customer", payload)
                continue
            seen_cust_sids.add(clean_sid)
            customers_by_id[customer_id] = (
                customer_id,
                city_id,
                segment,
                created_at,
                clean_sid,
                bronze_id,
                run_id,
            )

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.customers (
                customer_id, city_id, customer_segment, created_at_utc,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (customer_id) DO UPDATE SET
                city_id = EXCLUDED.city_id,
                customer_segment = EXCLUDED.customer_segment,
                created_at_utc = EXCLUDED.created_at_utc,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.customers (
                customer_id, city_id, customer_segment, created_at_utc,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO UPDATE SET
                city_id = EXCLUDED.city_id,
                customer_segment = EXCLUDED.customer_segment,
                created_at_utc = EXCLUDED.created_at_utc,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(customers_by_id.values()),
        )
        valid_customer_ids = set(customers_by_id.keys())
        loaded_stats["silver.customers"] = len(customers_by_id)

        # =====================================================================
        # 5. silver.customer_profiles (SCD2)
        # =====================================================================
        raw_profiles = _fetch_bronze_rows(cur, "bronze.raw_customer_profiles", pipeline_run_id)
        profiles_grouped: dict[str, list[dict[str, Any]]] = {}
        seen_profile_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_profiles:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            customer_id = str(payload.get("customer_id") or "").strip()
            city_id = str(payload.get("city_id") or "").strip().upper() or None
            segment = str(payload.get("customer_segment") or "").strip().lower() or None
            valid_from = _parse_utc_timestamp(payload.get("valid_from_utc"))
            valid_to = _parse_utc_timestamp(payload.get("valid_to_utc"))

            if not customer_id or not clean_sid or valid_from is None:
                rejections.add("silver.customer_profiles", s_row_id, bronze_id, "missing_required_profile_fields", payload)
                continue
            if customer_id not in valid_customer_ids:
                rejections.add("silver.customer_profiles", s_row_id, bronze_id, "missing_fk_customer_id", payload)
                continue
            if city_id and valid_city_ids and city_id not in valid_city_ids:
                rejections.add("silver.customer_profiles", s_row_id, bronze_id, "missing_fk_city_id", payload)
                continue
            if valid_to is not None and valid_to < valid_from:
                rejections.add("silver.customer_profiles", s_row_id, bronze_id, "invalid_scd2_date_range", payload)
                continue
            if clean_sid in seen_profile_sids:
                rejections.add("silver.customer_profiles", s_row_id, bronze_id, "duplicate_profile_source_row_id", payload)
                continue

            seen_profile_sids.add(clean_sid)
            profiles_grouped.setdefault(customer_id, []).append(
                {
                    "customer_id": customer_id,
                    "city_id": city_id,
                    "customer_segment": segment,
                    "valid_from_utc": valid_from,
                    "valid_to_utc": valid_to,
                    "source_row_id": clean_sid,
                    "bronze_id": bronze_id,
                    "pipeline_run_id": run_id,
                }
            )

        normalized_profiles = _normalize_scd2_versions(profiles_grouped)
        profile_tuples = [
            (
                p["customer_id"],
                p["city_id"],
                p["customer_segment"],
                p["valid_from_utc"],
                p["valid_to_utc"],
                p["source_row_id"],
                p["bronze_id"],
                p["pipeline_run_id"],
            )
            for p in normalized_profiles
        ]
        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.customer_profiles (
                customer_id, city_id, customer_segment, valid_from_utc,
                valid_to_utc, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (source_row_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                city_id = EXCLUDED.city_id,
                customer_segment = EXCLUDED.customer_segment,
                valid_from_utc = EXCLUDED.valid_from_utc,
                valid_to_utc = EXCLUDED.valid_to_utc,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.customer_profiles (
                customer_id, city_id, customer_segment, valid_from_utc,
                valid_to_utc, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_row_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                city_id = EXCLUDED.city_id,
                customer_segment = EXCLUDED.customer_segment,
                valid_from_utc = EXCLUDED.valid_from_utc,
                valid_to_utc = EXCLUDED.valid_to_utc,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            profile_tuples,
        )
        loaded_stats["silver.customer_profiles"] = len(profile_tuples)

        # =====================================================================
        # 6. silver.customer_addresses (SCD2)
        # =====================================================================
        raw_addresses = _fetch_bronze_rows(cur, "bronze.raw_customer_addresses", pipeline_run_id)
        addresses_grouped: dict[str, list[dict[str, Any]]] = {}
        seen_addr_ids: set[str] = set()
        seen_addr_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_addresses:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            address_id = str(payload.get("address_id") or "").strip()
            customer_id = str(payload.get("customer_id") or "").strip()
            city_id = str(payload.get("city_id") or "").strip().upper() or None
            valid_from = _parse_utc_timestamp(payload.get("valid_from_utc"))
            valid_to = _parse_utc_timestamp(payload.get("valid_to_utc"))

            if not address_id or not customer_id or not clean_sid or valid_from is None:
                rejections.add("silver.customer_addresses", s_row_id, bronze_id, "missing_required_address_fields", payload)
                continue
            if customer_id not in valid_customer_ids:
                rejections.add("silver.customer_addresses", s_row_id, bronze_id, "missing_fk_customer_id", payload)
                continue
            if city_id and valid_city_ids and city_id not in valid_city_ids:
                rejections.add("silver.customer_addresses", s_row_id, bronze_id, "missing_fk_city_id", payload)
                continue
            if valid_to is not None and valid_to < valid_from:
                rejections.add("silver.customer_addresses", s_row_id, bronze_id, "invalid_scd2_date_range", payload)
                continue
            if address_id in seen_addr_ids or clean_sid in seen_addr_sids:
                rejections.add("silver.customer_addresses", s_row_id, bronze_id, "duplicate_address", payload)
                continue

            seen_addr_ids.add(address_id)
            seen_addr_sids.add(clean_sid)
            addresses_grouped.setdefault(customer_id, []).append(
                {
                    "address_id": address_id,
                    "customer_id": customer_id,
                    "city_id": city_id,
                    "valid_from_utc": valid_from,
                    "valid_to_utc": valid_to,
                    "source_row_id": clean_sid,
                    "bronze_id": bronze_id,
                    "pipeline_run_id": run_id,
                }
            )

        normalized_addresses = _normalize_scd2_versions(addresses_grouped)
        address_tuples = [
            (
                a["address_id"],
                a["customer_id"],
                a["city_id"],
                a["valid_from_utc"],
                a["valid_to_utc"],
                a["source_row_id"],
                a["bronze_id"],
                a["pipeline_run_id"],
            )
            for a in normalized_addresses
        ]
        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.customer_addresses (
                address_id, customer_id, city_id, valid_from_utc,
                valid_to_utc, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (address_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                city_id = EXCLUDED.city_id,
                valid_from_utc = EXCLUDED.valid_from_utc,
                valid_to_utc = EXCLUDED.valid_to_utc,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.customer_addresses (
                address_id, customer_id, city_id, valid_from_utc,
                valid_to_utc, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (address_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                city_id = EXCLUDED.city_id,
                valid_from_utc = EXCLUDED.valid_from_utc,
                valid_to_utc = EXCLUDED.valid_to_utc,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            address_tuples,
        )
        loaded_stats["silver.customer_addresses"] = len(address_tuples)

        # =====================================================================
        # 7. silver.products
        # =====================================================================
        raw_products = _fetch_bronze_rows(cur, "bronze.raw_products", pipeline_run_id)
        products_by_id: dict[str, tuple[Any, ...]] = {}
        seen_prod_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_products:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            product_id = str(payload.get("product_id") or "").strip()
            product_name = str(payload.get("product_name") or "").strip() or None
            sku = str(payload.get("sku") or "").strip() or None
            unit_price = _parse_decimal(payload.get("unit_price"))

            if not product_id or not clean_sid or unit_price is None:
                rejections.add("silver.products", s_row_id, bronze_id, "missing_required_product_fields", payload)
                continue
            if unit_price < Decimal("0"):
                rejections.add("silver.products", s_row_id, bronze_id, "negative_unit_price", payload)
                continue
            if product_id in products_by_id or clean_sid in seen_prod_sids:
                rejections.add("silver.products", s_row_id, bronze_id, "duplicate_product", payload)
                continue

            seen_prod_sids.add(clean_sid)
            products_by_id[product_id] = (
                product_id,
                product_name,
                sku,
                unit_price,
                clean_sid,
                bronze_id,
                run_id,
            )

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.products (
                product_id, product_name, sku, unit_price,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                sku = EXCLUDED.sku,
                unit_price = EXCLUDED.unit_price,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.products (
                product_id, product_name, sku, unit_price,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                sku = EXCLUDED.sku,
                unit_price = EXCLUDED.unit_price,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(products_by_id.values()),
        )
        valid_product_ids = set(products_by_id.keys())
        loaded_stats["silver.products"] = len(products_by_id)

        # =====================================================================
        # 8. silver.product_categories (SCD2)
        # =====================================================================
        raw_categories = _fetch_bronze_rows(cur, "bronze.raw_product_categories", pipeline_run_id)
        categories_grouped: dict[str, list[dict[str, Any]]] = {}
        seen_cat_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_categories:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            product_id = str(payload.get("product_id") or "").strip()
            category_id = str(payload.get("category_id") or "").strip() or None
            category_name = str(payload.get("category_name") or "").strip().lower() or None
            valid_from = _parse_utc_timestamp(payload.get("valid_from_utc"))
            valid_to = _parse_utc_timestamp(payload.get("valid_to_utc"))

            if not product_id or not clean_sid or valid_from is None:
                rejections.add("silver.product_categories", s_row_id, bronze_id, "missing_required_category_fields", payload)
                continue
            if product_id not in valid_product_ids:
                rejections.add("silver.product_categories", s_row_id, bronze_id, "missing_fk_product_id", payload)
                continue
            if valid_to is not None and valid_to < valid_from:
                rejections.add("silver.product_categories", s_row_id, bronze_id, "invalid_scd2_date_range", payload)
                continue
            if clean_sid in seen_cat_sids:
                rejections.add("silver.product_categories", s_row_id, bronze_id, "duplicate_category_source_row_id", payload)
                continue

            seen_cat_sids.add(clean_sid)
            categories_grouped.setdefault(product_id, []).append(
                {
                    "product_id": product_id,
                    "category_id": category_id,
                    "category_name": category_name,
                    "valid_from_utc": valid_from,
                    "valid_to_utc": valid_to,
                    "source_row_id": clean_sid,
                    "bronze_id": bronze_id,
                    "pipeline_run_id": run_id,
                }
            )

        normalized_categories = _normalize_scd2_versions(categories_grouped)
        category_tuples = [
            (
                c["product_id"],
                c["category_id"],
                c["category_name"],
                c["valid_from_utc"],
                c["valid_to_utc"],
                c["source_row_id"],
                c["bronze_id"],
                c["pipeline_run_id"],
            )
            for c in normalized_categories
        ]
        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.product_categories (
                product_id, category_id, category_name, valid_from_utc,
                valid_to_utc, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (source_row_id) DO UPDATE SET
                product_id = EXCLUDED.product_id,
                category_id = EXCLUDED.category_id,
                category_name = EXCLUDED.category_name,
                valid_from_utc = EXCLUDED.valid_from_utc,
                valid_to_utc = EXCLUDED.valid_to_utc,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.product_categories (
                product_id, category_id, category_name, valid_from_utc,
                valid_to_utc, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_row_id) DO UPDATE SET
                product_id = EXCLUDED.product_id,
                category_id = EXCLUDED.category_id,
                category_name = EXCLUDED.category_name,
                valid_from_utc = EXCLUDED.valid_from_utc,
                valid_to_utc = EXCLUDED.valid_to_utc,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            category_tuples,
        )
        loaded_stats["silver.product_categories"] = len(category_tuples)

        # =====================================================================
        # 9. silver.promotions
        # =====================================================================
        raw_promotions = _fetch_bronze_rows(cur, "bronze.raw_promotions", pipeline_run_id)
        promotions_by_id: dict[str, tuple[Any, ...]] = {}
        seen_promo_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_promotions:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            promo_id = str(payload.get("promotion_id") or "").strip()
            promo_code = str(payload.get("promotion_code") or "").strip() or None
            promo_type = str(payload.get("promotion_type") or "").strip().lower() or None
            discount_rate = _parse_decimal(payload.get("discount_rate"))
            start_dt = _parse_date(payload.get("start_date"))
            end_dt = _parse_date(payload.get("end_date"))

            if not promo_id or not clean_sid:
                rejections.add("silver.promotions", s_row_id, bronze_id, "missing_promotion_id", payload)
                continue
            if discount_rate is not None and discount_rate < Decimal("0"):
                rejections.add("silver.promotions", s_row_id, bronze_id, "negative_discount_rate", payload)
                continue
            if start_dt and end_dt and end_dt < start_dt:
                rejections.add("silver.promotions", s_row_id, bronze_id, "invalid_promotion_date_range", payload)
                continue
            if promo_id in promotions_by_id or clean_sid in seen_promo_sids:
                rejections.add("silver.promotions", s_row_id, bronze_id, "duplicate_promotion", payload)
                continue

            seen_promo_sids.add(clean_sid)
            promotions_by_id[promo_id] = (
                promo_id,
                promo_code,
                promo_type,
                discount_rate,
                start_dt,
                end_dt,
                clean_sid,
                bronze_id,
                run_id,
            )

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.promotions (
                promotion_id, promotion_code, promotion_type, discount_rate,
                start_date, end_date, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (promotion_id) DO UPDATE SET
                promotion_code = EXCLUDED.promotion_code,
                promotion_type = EXCLUDED.promotion_type,
                discount_rate = EXCLUDED.discount_rate,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.promotions (
                promotion_id, promotion_code, promotion_type, discount_rate,
                start_date, end_date, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (promotion_id) DO UPDATE SET
                promotion_code = EXCLUDED.promotion_code,
                promotion_type = EXCLUDED.promotion_type,
                discount_rate = EXCLUDED.discount_rate,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(promotions_by_id.values()),
        )
        valid_promotion_ids = set(promotions_by_id.keys())
        loaded_stats["silver.promotions"] = len(promotions_by_id)

        # =====================================================================
        # 10. silver.orders (deterministic deduplication + status normalization)
        # =====================================================================
        raw_orders = _fetch_bronze_rows(cur, "bronze.raw_orders", pipeline_run_id)
        # Group candidate orders by order_id for deterministic deduplication
        order_candidates_by_id: dict[str, list[tuple[int, str, dict[str, Any]]]] = {}
        for bronze_id, s_row_id, payload, _ing_at in raw_orders:
            order_id = str(payload.get("order_id") or "").strip()
            if not order_id:
                rejections.add("silver.orders", s_row_id, bronze_id, "missing_order_id", payload)
                continue
            order_candidates_by_id.setdefault(order_id, []).append((bronze_id, s_row_id, payload))

        orders_by_id: dict[str, tuple[Any, ...]] = {}
        seen_order_sids: set[str] = set()

        for order_id, candidates in order_candidates_by_id.items():
            # Deterministic ordering: prefer non-DUPLICATE source_row_id, latest updated_at_utc, lowest source_row_id, lowest bronze_id
            candidates.sort(
                key=lambda item: (
                    _is_explicit_duplicate_row_id(item[2].get("source_row_id") or item[1]),
                    -(
                        (
                            _parse_utc_timestamp(item[2].get("updated_at_utc"))
                            or _parse_utc_timestamp(item[2].get("ordered_at_utc"))
                            or datetime.min.replace(tzinfo=timezone.utc)
                        ).timestamp()
                    ),
                    _clean_source_row_id(item[2].get("source_row_id") or item[1]),
                    item[0],
                )
            )
            winner_bronze_id, winner_s_row_id, payload = candidates[0]
            for dup_bronze_id, dup_s_row_id, dup_payload in candidates[1:]:
                rejections.add("silver.orders", dup_s_row_id, dup_bronze_id, "duplicate_order_id", dup_payload)

            clean_sid = _clean_source_row_id(payload.get("source_row_id") or winner_s_row_id)
            customer_id = str(payload.get("customer_id") or "").strip()
            ordered_at = _parse_utc_timestamp(payload.get("ordered_at_utc"))
            updated_at = _parse_utc_timestamp(payload.get("updated_at_utc"))
            sales_channel = str(payload.get("sales_channel") or "").strip().upper()
            raw_store = payload.get("store_id")
            store_id = str(raw_store).strip() if raw_store not in (None, "") else None
            normalized_status = _normalize_order_status(payload.get("status"))
            shipping_revenue = _parse_decimal(payload.get("shipping_revenue"), Decimal("0.00"))

            if not customer_id or ordered_at is None or not sales_channel or not clean_sid:
                rejections.add("silver.orders", winner_s_row_id, winner_bronze_id, "missing_required_order_fields", payload)
                continue
            if normalized_status is None:
                rejections.add("silver.orders", winner_s_row_id, winner_bronze_id, "invalid_order_status", payload)
                continue
            if shipping_revenue is None or shipping_revenue < Decimal("0"):
                rejections.add("silver.orders", winner_s_row_id, winner_bronze_id, "negative_shipping_revenue", payload)
                continue
            if customer_id not in valid_customer_ids:
                rejections.add("silver.orders", winner_s_row_id, winner_bronze_id, "missing_fk_customer_id", payload)
                continue
            if sales_channel not in valid_channel_ids:
                rejections.add("silver.orders", winner_s_row_id, winner_bronze_id, "missing_fk_sales_channel", payload)
                continue
            if store_id is not None and store_id not in valid_store_ids:
                rejections.add("silver.orders", winner_s_row_id, winner_bronze_id, "missing_fk_store_id", payload)
                continue
            if clean_sid in seen_order_sids:
                rejections.add("silver.orders", winner_s_row_id, winner_bronze_id, "duplicate_order_source_row_id", payload)
                continue

            seen_order_sids.add(clean_sid)
            orders_by_id[order_id] = (
                order_id,
                customer_id,
                ordered_at,
                updated_at,
                sales_channel,
                store_id,
                normalized_status,
                shipping_revenue,
                clean_sid,
                winner_bronze_id,
                run_id,
            )

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.orders (
                order_id, customer_id, ordered_at_utc, updated_at_utc,
                sales_channel, store_id, status, shipping_revenue,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                ordered_at_utc = EXCLUDED.ordered_at_utc,
                updated_at_utc = EXCLUDED.updated_at_utc,
                sales_channel = EXCLUDED.sales_channel,
                store_id = EXCLUDED.store_id,
                status = EXCLUDED.status,
                shipping_revenue = EXCLUDED.shipping_revenue,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.orders (
                order_id, customer_id, ordered_at_utc, updated_at_utc,
                sales_channel, store_id, status, shipping_revenue,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                ordered_at_utc = EXCLUDED.ordered_at_utc,
                updated_at_utc = EXCLUDED.updated_at_utc,
                sales_channel = EXCLUDED.sales_channel,
                store_id = EXCLUDED.store_id,
                status = EXCLUDED.status,
                shipping_revenue = EXCLUDED.shipping_revenue,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(orders_by_id.values()),
        )
        valid_order_ids = set(orders_by_id.keys())
        loaded_stats["silver.orders"] = len(orders_by_id)

        # =====================================================================
        # 11. silver.order_items (reject negative/zero qty & missing FKs)
        # =====================================================================
        raw_items = _fetch_bronze_rows(cur, "bronze.raw_order_items", pipeline_run_id)
        items_by_id: dict[str, tuple[Any, ...]] = {}
        seen_item_sids: set[str] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_items:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            order_item_id = str(payload.get("order_item_id") or "").strip()
            order_id = str(payload.get("order_id") or "").strip()
            product_id = str(payload.get("product_id") or "").strip()
            quantity = _parse_int(payload.get("quantity"))
            unit_price = _parse_decimal(payload.get("unit_price"))
            discount_amt = _parse_decimal(payload.get("item_discount_amount"), Decimal("0.00"))

            if not order_item_id or not order_id or not product_id or not clean_sid:
                rejections.add("silver.order_items", s_row_id, bronze_id, "missing_required_order_item_fields", payload)
                continue
            if quantity is None or quantity <= 0:
                rejections.add("silver.order_items", s_row_id, bronze_id, "invalid_negative_or_zero_quantity", payload)
                continue
            if unit_price is None or unit_price < Decimal("0"):
                rejections.add("silver.order_items", s_row_id, bronze_id, "invalid_unit_price", payload)
                continue
            if discount_amt is None or discount_amt < Decimal("0"):
                rejections.add("silver.order_items", s_row_id, bronze_id, "negative_item_discount_amount", payload)
                continue
            if order_id not in valid_order_ids:
                rejections.add("silver.order_items", s_row_id, bronze_id, "missing_fk_order_id", payload)
                continue
            if product_id not in valid_product_ids:
                rejections.add("silver.order_items", s_row_id, bronze_id, "missing_fk_product_id", payload)
                continue
            if order_item_id in items_by_id or clean_sid in seen_item_sids:
                rejections.add("silver.order_items", s_row_id, bronze_id, "duplicate_order_item", payload)
                continue

            seen_item_sids.add(clean_sid)
            items_by_id[order_item_id] = (
                order_item_id,
                order_id,
                product_id,
                quantity,
                unit_price,
                discount_amt,
                clean_sid,
                bronze_id,
                run_id,
            )

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.order_items (
                order_item_id, order_id, product_id, quantity,
                unit_price, item_discount_amount, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (order_item_id) DO UPDATE SET
                order_id = EXCLUDED.order_id,
                product_id = EXCLUDED.product_id,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                item_discount_amount = EXCLUDED.item_discount_amount,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.order_items (
                order_item_id, order_id, product_id, quantity,
                unit_price, item_discount_amount, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_item_id) DO UPDATE SET
                order_id = EXCLUDED.order_id,
                product_id = EXCLUDED.product_id,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                item_discount_amount = EXCLUDED.item_discount_amount,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(items_by_id.values()),
        )
        loaded_stats["silver.order_items"] = len(items_by_id)

        # =====================================================================
        # 12. silver.order_promotions
        # =====================================================================
        raw_order_promos = _fetch_bronze_rows(cur, "bronze.raw_order_promotions", pipeline_run_id)
        order_promos_list: list[tuple[Any, ...]] = []
        seen_opromo_sids: set[str] = set()
        seen_order_promo_pairs: set[tuple[str, str]] = set()
        for bronze_id, s_row_id, payload, _ing_at in raw_order_promos:
            clean_sid = _clean_source_row_id(payload.get("source_row_id") or s_row_id)
            order_id = str(payload.get("order_id") or "").strip()
            promo_id = str(payload.get("promotion_id") or "").strip()
            discount_amt = _parse_decimal(payload.get("discount_amount"), Decimal("0.00"))

            if not order_id or not promo_id or not clean_sid:
                rejections.add("silver.order_promotions", s_row_id, bronze_id, "missing_required_order_promo_fields", payload)
                continue
            if discount_amt is None or discount_amt < Decimal("0"):
                rejections.add("silver.order_promotions", s_row_id, bronze_id, "negative_promotion_discount_amount", payload)
                continue
            if order_id not in valid_order_ids:
                rejections.add("silver.order_promotions", s_row_id, bronze_id, "missing_fk_order_id", payload)
                continue
            if promo_id not in valid_promotion_ids:
                rejections.add("silver.order_promotions", s_row_id, bronze_id, "missing_fk_promotion_id", payload)
                continue
            if clean_sid in seen_opromo_sids or (order_id, promo_id) in seen_order_promo_pairs:
                rejections.add("silver.order_promotions", s_row_id, bronze_id, "duplicate_order_promotion", payload)
                continue

            seen_opromo_sids.add(clean_sid)
            seen_order_promo_pairs.add((order_id, promo_id))
            order_promos_list.append((order_id, promo_id, discount_amt, clean_sid, bronze_id, run_id))

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.order_promotions (
                order_id, promotion_id, discount_amount,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (source_row_id) DO UPDATE SET
                order_id = EXCLUDED.order_id,
                promotion_id = EXCLUDED.promotion_id,
                discount_amount = EXCLUDED.discount_amount,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.order_promotions (
                order_id, promotion_id, discount_amount,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_row_id) DO UPDATE SET
                order_id = EXCLUDED.order_id,
                promotion_id = EXCLUDED.promotion_id,
                discount_amount = EXCLUDED.discount_amount,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            order_promos_list,
        )
        loaded_stats["silver.order_promotions"] = len(order_promos_list)

        # =====================================================================
        # Event Identity Resolution Helper
        # =====================================================================
        resolved_event_identities: list[tuple[str, int, str, datetime]] = []

        def _dedup_event_rows(
            bronze_table: str,
            silver_table: str,
        ) -> list[tuple[int, str, dict[str, Any], datetime, datetime | None]]:
            raw_ev = _fetch_bronze_rows(cur, bronze_table, pipeline_run_id)
            by_eid: dict[str, list[tuple[int, str, dict[str, Any], datetime, datetime | None]]] = {}
            for bronze_id, s_row_id, payload, bronze_ingested_at in raw_ev:
                event_id = str(payload.get("event_id") or "").strip()
                occurred_at = _parse_utc_timestamp(payload.get("occurred_at_utc"))
                ingested_at = _parse_utc_timestamp(payload.get("ingested_at_utc")) or bronze_ingested_at
                if not event_id or occurred_at is None:
                    rejections.add(silver_table, s_row_id, bronze_id, "missing_event_id_or_occurred_at", payload)
                    continue
                by_eid.setdefault(event_id, []).append(
                    (bronze_id, s_row_id, payload, occurred_at, ingested_at)
                )

            winners: list[tuple[int, str, dict[str, Any], datetime, datetime | None]] = []
            for event_id, candidates in by_eid.items():
                # Deterministic deduplication: earliest ingested_at_utc, canonical source_row_id, lowest bronze_id
                candidates.sort(
                    key=lambda item: (
                        _is_explicit_duplicate_row_id(item[1]),
                        item[4] or datetime.max.replace(tzinfo=timezone.utc),
                        item[0],
                    )
                )
                win = candidates[0]
                winners.append(win)
                for dup in candidates[1:]:
                    rejections.add(silver_table, dup[1], dup[0], "duplicate_event_id", dup[2])
            # Sort winners chronologically by occurred_at_utc to handle late-arriving / out-of-order events
            winners.sort(key=lambda item: (item[3], item[0]))
            return winners

        # =====================================================================
        # 13. silver.payment_events
        # =====================================================================
        payment_winners = _dedup_event_rows("bronze.raw_payment_events", "silver.payment_events")
        payment_tuples: list[tuple[Any, ...]] = []
        for bronze_id, s_row_id, payload, occurred_at, ingested_at in payment_winners:
            event_id = str(payload["event_id"]).strip()
            event_type = str(payload.get("event_type") or "").strip().upper()
            nested = payload.get("payload") or {}
            payment_id = str(nested.get("payment_id") or payload.get("payment_id") or "").strip()
            order_id = str(nested.get("order_id") or payload.get("order_id") or "").strip()
            amount = _parse_decimal(nested.get("amount") if "amount" in nested else payload.get("amount"))
            clean_sid = _clean_source_row_id(s_row_id) or event_id

            if not event_type or not payment_id or not order_id or amount is None:
                rejections.add("silver.payment_events", s_row_id, bronze_id, "missing_required_payment_fields", payload)
                continue
            if amount < Decimal("0"):
                rejections.add("silver.payment_events", s_row_id, bronze_id, "negative_payment_amount", payload)
                continue
            if order_id not in valid_order_ids:
                rejections.add("silver.payment_events", s_row_id, bronze_id, "missing_fk_order_id", payload)
                continue

            payment_tuples.append(
                (event_id, event_type, occurred_at, ingested_at, payment_id, order_id, amount, clean_sid, bronze_id, run_id)
            )
            resolved_event_identities.append((clean_sid, bronze_id, run_id, occurred_at))

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.payment_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                payment_id, order_id, amount, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                payment_id = EXCLUDED.payment_id,
                order_id = EXCLUDED.order_id,
                amount = EXCLUDED.amount,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.payment_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                payment_id, order_id, amount, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                payment_id = EXCLUDED.payment_id,
                order_id = EXCLUDED.order_id,
                amount = EXCLUDED.amount,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            payment_tuples,
        )
        loaded_stats["silver.payment_events"] = len(payment_tuples)

        # =====================================================================
        # 14. silver.refund_events
        # =====================================================================
        refund_winners = _dedup_event_rows("bronze.raw_refund_events", "silver.refund_events")
        refund_tuples: list[tuple[Any, ...]] = []
        for bronze_id, s_row_id, payload, occurred_at, ingested_at in refund_winners:
            event_id = str(payload["event_id"]).strip()
            event_type = str(payload.get("event_type") or "").strip().upper()
            nested = payload.get("payload") or {}
            refund_id = str(nested.get("refund_id") or payload.get("refund_id") or "").strip()
            order_id = str(nested.get("order_id") or payload.get("order_id") or "").strip()
            amount = _parse_decimal(nested.get("amount") if "amount" in nested else payload.get("amount"))
            clean_sid = _clean_source_row_id(s_row_id) or event_id

            if not event_type or not refund_id or not order_id or amount is None:
                rejections.add("silver.refund_events", s_row_id, bronze_id, "missing_required_refund_fields", payload)
                continue
            if amount < Decimal("0"):
                rejections.add("silver.refund_events", s_row_id, bronze_id, "negative_refund_amount", payload)
                continue
            if order_id not in valid_order_ids:
                rejections.add("silver.refund_events", s_row_id, bronze_id, "missing_fk_order_id", payload)
                continue

            refund_tuples.append(
                (event_id, event_type, occurred_at, ingested_at, refund_id, order_id, amount, clean_sid, bronze_id, run_id)
            )
            resolved_event_identities.append((clean_sid, bronze_id, run_id, occurred_at))

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.refund_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                refund_id, order_id, amount, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                refund_id = EXCLUDED.refund_id,
                order_id = EXCLUDED.order_id,
                amount = EXCLUDED.amount,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.refund_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                refund_id, order_id, amount, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                refund_id = EXCLUDED.refund_id,
                order_id = EXCLUDED.order_id,
                amount = EXCLUDED.amount,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            refund_tuples,
        )
        loaded_stats["silver.refund_events"] = len(refund_tuples)

        # =====================================================================
        # 15. silver.return_events
        # =====================================================================
        return_winners = _dedup_event_rows("bronze.raw_return_events", "silver.return_events")
        return_tuples: list[tuple[Any, ...]] = []
        for bronze_id, s_row_id, payload, occurred_at, ingested_at in return_winners:
            event_id = str(payload["event_id"]).strip()
            event_type = str(payload.get("event_type") or "").strip().upper()
            nested = payload.get("payload") or {}
            return_id = str(nested.get("return_id") or payload.get("return_id") or "").strip()
            order_id = str(nested.get("order_id") or payload.get("order_id") or "").strip()
            clean_sid = _clean_source_row_id(s_row_id) or event_id

            if not event_type or not return_id or not order_id:
                rejections.add("silver.return_events", s_row_id, bronze_id, "missing_required_return_fields", payload)
                continue
            if order_id not in valid_order_ids:
                rejections.add("silver.return_events", s_row_id, bronze_id, "missing_fk_order_id", payload)
                continue

            return_tuples.append(
                (event_id, event_type, occurred_at, ingested_at, return_id, order_id, clean_sid, bronze_id, run_id)
            )
            resolved_event_identities.append((clean_sid, bronze_id, run_id, occurred_at))

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.return_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                return_id, order_id, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                return_id = EXCLUDED.return_id,
                order_id = EXCLUDED.order_id,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.return_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                return_id, order_id, source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                return_id = EXCLUDED.return_id,
                order_id = EXCLUDED.order_id,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            return_tuples,
        )
        loaded_stats["silver.return_events"] = len(return_tuples)

        # =====================================================================
        # 16. silver.support_events
        # =====================================================================
        support_winners = _dedup_event_rows("bronze.raw_support_events", "silver.support_events")
        support_tuples: list[tuple[Any, ...]] = []
        for bronze_id, s_row_id, payload, occurred_at, ingested_at in support_winners:
            event_id = str(payload["event_id"]).strip()
            event_type = str(payload.get("event_type") or "").strip().upper()
            nested = payload.get("payload") or {}
            ticket_id = str(nested.get("ticket_id") or payload.get("ticket_id") or "").strip()
            raw_oid = nested.get("order_id") or payload.get("order_id")
            order_id = str(raw_oid).strip() if raw_oid not in (None, "") else None
            customer_id = str(nested.get("customer_id") or payload.get("customer_id") or "").strip()
            reason = str(nested.get("reason") or payload.get("reason") or "").strip().lower() or None
            clean_sid = _clean_source_row_id(s_row_id) or event_id

            if not event_type or not ticket_id or not customer_id:
                rejections.add("silver.support_events", s_row_id, bronze_id, "missing_required_support_fields", payload)
                continue
            if customer_id not in valid_customer_ids:
                rejections.add("silver.support_events", s_row_id, bronze_id, "missing_fk_customer_id", payload)
                continue
            if order_id is not None and order_id not in valid_order_ids:
                rejections.add("silver.support_events", s_row_id, bronze_id, "missing_fk_order_id", payload)
                continue

            support_tuples.append(
                (event_id, event_type, occurred_at, ingested_at, ticket_id, order_id, customer_id, reason, clean_sid, bronze_id, run_id)
            )
            resolved_event_identities.append((clean_sid, bronze_id, run_id, occurred_at))

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.support_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                ticket_id, order_id, customer_id, reason,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                ticket_id = EXCLUDED.ticket_id,
                order_id = EXCLUDED.order_id,
                customer_id = EXCLUDED.customer_id,
                reason = EXCLUDED.reason,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.support_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                ticket_id, order_id, customer_id, reason,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                ticket_id = EXCLUDED.ticket_id,
                order_id = EXCLUDED.order_id,
                customer_id = EXCLUDED.customer_id,
                reason = EXCLUDED.reason,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            support_tuples,
        )
        loaded_stats["silver.support_events"] = len(support_tuples)

        # =====================================================================
        # 17. silver.web_events
        # =====================================================================
        web_winners = _dedup_event_rows("bronze.raw_web_events", "silver.web_events")
        web_tuples: list[tuple[Any, ...]] = []
        for bronze_id, s_row_id, payload, occurred_at, ingested_at in web_winners:
            event_id = str(payload["event_id"]).strip()
            event_type = str(payload.get("event_type") or "").strip().upper()
            nested = payload.get("payload") or {}
            raw_sess = nested.get("session_id") or payload.get("session_id")
            session_id = str(raw_sess).strip() if raw_sess not in (None, "") else None
            raw_cid = nested.get("customer_id") or payload.get("customer_id")
            customer_id = str(raw_cid).strip() if raw_cid not in (None, "") else None
            raw_oid = nested.get("order_id") or payload.get("order_id")
            order_id = str(raw_oid).strip() if raw_oid not in (None, "") else None
            raw_ch = nested.get("channel") or payload.get("channel")
            channel = str(raw_ch).strip().upper() if raw_ch not in (None, "") else None
            raw_cmp = nested.get("campaign_id") or payload.get("campaign_id")
            campaign_id = str(raw_cmp).strip() if raw_cmp not in (None, "") else None
            clean_sid = _clean_source_row_id(s_row_id) or event_id

            if not event_type:
                rejections.add("silver.web_events", s_row_id, bronze_id, "missing_event_type", payload)
                continue
            if customer_id is not None and customer_id not in valid_customer_ids:
                rejections.add("silver.web_events", s_row_id, bronze_id, "missing_fk_customer_id", payload)
                continue
            if order_id is not None and order_id not in valid_order_ids:
                rejections.add("silver.web_events", s_row_id, bronze_id, "missing_fk_order_id", payload)
                continue
            if channel is not None and channel not in valid_channel_ids:
                rejections.add("silver.web_events", s_row_id, bronze_id, "missing_fk_channel", payload)
                continue

            web_tuples.append(
                (event_id, event_type, occurred_at, ingested_at, session_id, customer_id, order_id, channel, campaign_id, clean_sid, bronze_id, run_id)
            )
            resolved_event_identities.append((clean_sid, bronze_id, run_id, occurred_at))

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.web_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                session_id, customer_id, order_id, channel, campaign_id,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                session_id = EXCLUDED.session_id,
                customer_id = EXCLUDED.customer_id,
                order_id = EXCLUDED.order_id,
                channel = EXCLUDED.channel,
                campaign_id = EXCLUDED.campaign_id,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.web_events (
                event_id, event_type, occurred_at_utc, ingested_at_utc,
                session_id, customer_id, order_id, channel, campaign_id,
                source_row_id, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                occurred_at_utc = EXCLUDED.occurred_at_utc,
                ingested_at_utc = EXCLUDED.ingested_at_utc,
                session_id = EXCLUDED.session_id,
                customer_id = EXCLUDED.customer_id,
                order_id = EXCLUDED.order_id,
                channel = EXCLUDED.channel,
                campaign_id = EXCLUDED.campaign_id,
                source_row_id = EXCLUDED.source_row_id,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            web_tuples,
        )
        loaded_stats["silver.web_events"] = len(web_tuples)

        # Populate silver.event_identity if the table exists in the schema
        cur.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = 'silver' AND table_name = 'event_identity'
            """
        )
        if cur.fetchone() and resolved_event_identities:
            _exec_batch(
                cur,
                driver,
                """
                INSERT INTO silver.event_identity (source_row_id, bronze_id, pipeline_run_id, created_at_utc)
                VALUES %s
                ON CONFLICT (source_row_id) DO UPDATE SET
                    bronze_id = EXCLUDED.bronze_id,
                    pipeline_run_id = EXCLUDED.pipeline_run_id,
                    created_at_utc = EXCLUDED.created_at_utc
                """,
                """
                INSERT INTO silver.event_identity (source_row_id, bronze_id, pipeline_run_id, created_at_utc)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (source_row_id) DO UPDATE SET
                    bronze_id = EXCLUDED.bronze_id,
                    pipeline_run_id = EXCLUDED.pipeline_run_id,
                    created_at_utc = EXCLUDED.created_at_utc
                """,
                resolved_event_identities,
            )

        # =====================================================================
        # 18. silver.inventory_snapshots
        # =====================================================================
        raw_inv = _fetch_bronze_rows(cur, "bronze.raw_inventory_snapshots", pipeline_run_id)
        inv_by_key: dict[tuple[str, str, date], tuple[Any, ...]] = {}
        for bronze_id, s_row_id, payload, _ing_at in raw_inv:
            product_id = str(payload.get("product_id") or "").strip()
            location_id = str(payload.get("location_id") or "").strip()
            snapshot_dt = _parse_date(payload.get("snapshot_date"))
            avail_qty = _parse_int(payload.get("available_quantity"))
            res_qty = _parse_int(payload.get("reserved_quantity"))
            unit_cost = _parse_decimal(payload.get("unit_cost"))

            if not product_id or not location_id or snapshot_dt is None or avail_qty is None or res_qty is None:
                rejections.add("silver.inventory_snapshots", s_row_id, bronze_id, "missing_required_inventory_fields", payload)
                continue
            if avail_qty < 0 or res_qty < 0 or (unit_cost is not None and unit_cost < Decimal("0")):
                rejections.add("silver.inventory_snapshots", s_row_id, bronze_id, "negative_inventory_quantity_or_cost", payload)
                continue
            if product_id not in valid_product_ids:
                rejections.add("silver.inventory_snapshots", s_row_id, bronze_id, "missing_fk_product_id", payload)
                continue
            if location_id not in valid_store_ids:
                rejections.add("silver.inventory_snapshots", s_row_id, bronze_id, "missing_fk_location_id", payload)
                continue

            key = (product_id, location_id, snapshot_dt)
            if key in inv_by_key:
                rejections.add("silver.inventory_snapshots", s_row_id, bronze_id, "duplicate_inventory_snapshot", payload)
                continue

            inv_by_key[key] = (
                product_id,
                location_id,
                snapshot_dt,
                avail_qty,
                res_qty,
                unit_cost,
                bronze_id,
                run_id,
            )

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.inventory_snapshots (
                product_id, location_id, snapshot_date,
                available_quantity, reserved_quantity, unit_cost,
                bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (product_id, location_id, snapshot_date) DO UPDATE SET
                available_quantity = EXCLUDED.available_quantity,
                reserved_quantity = EXCLUDED.reserved_quantity,
                unit_cost = EXCLUDED.unit_cost,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.inventory_snapshots (
                product_id, location_id, snapshot_date,
                available_quantity, reserved_quantity, unit_cost,
                bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id, location_id, snapshot_date) DO UPDATE SET
                available_quantity = EXCLUDED.available_quantity,
                reserved_quantity = EXCLUDED.reserved_quantity,
                unit_cost = EXCLUDED.unit_cost,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(inv_by_key.values()),
        )
        loaded_stats["silver.inventory_snapshots"] = len(inv_by_key)

        # =====================================================================
        # 19. silver.campaign_spend
        # =====================================================================
        raw_spend = _fetch_bronze_rows(cur, "bronze.raw_campaign_spend", pipeline_run_id)
        spend_by_key: dict[tuple[date, str, str], tuple[Any, ...]] = {}
        for bronze_id, s_row_id, payload, _ing_at in raw_spend:
            spend_dt = _parse_date(payload.get("spend_date"))
            campaign_id = str(payload.get("campaign_id") or "").strip()
            channel = str(payload.get("channel") or "").strip().upper()
            spend_amt = _parse_decimal(payload.get("spend_amount"))

            if spend_dt is None or not campaign_id or not channel or spend_amt is None:
                rejections.add("silver.campaign_spend", s_row_id, bronze_id, "missing_required_campaign_spend_fields", payload)
                continue
            if spend_amt < Decimal("0"):
                rejections.add("silver.campaign_spend", s_row_id, bronze_id, "negative_spend_amount", payload)
                continue
            if channel not in valid_channel_ids:
                rejections.add("silver.campaign_spend", s_row_id, bronze_id, "missing_fk_channel", payload)
                continue

            key = (spend_dt, campaign_id, channel)
            if key in spend_by_key:
                rejections.add("silver.campaign_spend", s_row_id, bronze_id, "duplicate_campaign_spend", payload)
                continue

            spend_by_key[key] = (spend_dt, campaign_id, channel, spend_amt, bronze_id, run_id)

        _exec_batch(
            cur,
            driver,
            """
            INSERT INTO silver.campaign_spend (
                spend_date, campaign_id, channel, spend_amount, bronze_id, pipeline_run_id
            )
            VALUES %s
            ON CONFLICT (spend_date, campaign_id, channel) DO UPDATE SET
                spend_amount = EXCLUDED.spend_amount,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            """
            INSERT INTO silver.campaign_spend (
                spend_date, campaign_id, channel, spend_amount, bronze_id, pipeline_run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (spend_date, campaign_id, channel) DO UPDATE SET
                spend_amount = EXCLUDED.spend_amount,
                bronze_id = EXCLUDED.bronze_id,
                pipeline_run_id = EXCLUDED.pipeline_run_id
            """,
            list(spend_by_key.values()),
        )
        loaded_stats["silver.campaign_spend"] = len(spend_by_key)

        # =====================================================================
        # Persist rejections & ops metadata
        # =====================================================================
        total_rejected = rejections.flush(cur, driver)
        total_loaded = sum(loaded_stats.values())

        cur.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, status, silver_record_count, rejected_record_count)
            VALUES (%s, 'RUNNING', %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET
                silver_record_count = EXCLUDED.silver_record_count,
                rejected_record_count = EXCLUDED.rejected_record_count;
            """,
            (run_id, total_loaded, total_rejected),
        )

        cur.execute(
            "DELETE FROM ops.layer_load_stats WHERE pipeline_run_id = %s AND layer = 'silver'",
            (run_id,),
        )
        for tbl_name, loaded_cnt in loaded_stats.items():
            rej_cnt = rejections.per_table_counts.get(tbl_name, 0)
            cur.execute(
                """
                INSERT INTO ops.layer_load_stats (
                    pipeline_run_id, layer, table_name, records_loaded, records_rejected
                )
                VALUES (%s, 'silver', %s, %s, %s)
                """,
                (run_id, tbl_name, loaded_cnt, rej_cnt),
            )
            print(f"[Silver] {tbl_name}: loaded={loaded_cnt}, rejected={rej_cnt}")

        conn.commit()
        print(
            f"Silver pipeline completed successfully (run_id={run_id}, "
            f"loaded={total_loaded}, rejected={total_rejected})"
        )
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


# ---------------------------------------------------------------------------
# Silver Quality Gate (blocks Gold build if any critical check fails)
# ---------------------------------------------------------------------------
def run_quality_gate(pipeline_run_id: str | None = None) -> bool:
    """Execute Silver data quality checks and persist outcomes into ``ops.quality_check_results``.

    Categories verified:
    - ``uniqueness``: primary/business keys across Silver tables have zero duplicates
    - ``business_rule``: positive quantities, non-negative amounts, normalized order statuses, valid SCD2 intervals
    - ``referential_integrity``: all foreign key links (orders->customers/channels/stores, items->orders/products, events->orders) are intact
    - ``reconciliation``: Bronze raw rows equal Silver loaded + Silver rejected rows
    """
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()

    try:
        if not pipeline_run_id:
            cur.execute(
                "SELECT pipeline_run_id FROM silver.orders ORDER BY ingested_at_utc DESC LIMIT 1"
            )
            row = cur.fetchone()
            pipeline_run_id = str(row[0]) if row else _new_run_id()

        # Remove prior Silver quality gate checks for this run_id so re-runs stay idempotent
        cur.execute(
            """
            DELETE FROM ops.quality_check_results
            WHERE pipeline_run_id = %s AND target_table LIKE 'silver.%%'
            """,
            (pipeline_run_id,),
        )

        checks: list[tuple[str, str, str, str, str, Any]] = [
            (
                "silver_orders_not_empty",
                "business_rule",
                "silver.orders",
                "SELECT COUNT(*) FROM silver.orders",
                "> 0",
                lambda val: int(val) > 0,
            ),
            (
                "silver_orders_unique_order_id",
                "uniqueness",
                "silver.orders",
                "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM silver.orders",
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_payment_events_unique_event_id",
                "uniqueness",
                "silver.payment_events",
                "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM silver.payment_events",
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_refund_events_unique_event_id",
                "uniqueness",
                "silver.refund_events",
                "SELECT COUNT(*) - COUNT(DISTINCT event_id) FROM silver.refund_events",
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_order_items_positive_qty_and_valid_amounts",
                "business_rule",
                "silver.order_items",
                "SELECT COUNT(*) FROM silver.order_items WHERE quantity <= 0 OR unit_price < 0 OR item_discount_amount < 0",
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_orders_normalized_status_enum",
                "business_rule",
                "silver.orders",
                "SELECT COUNT(*) FROM silver.orders WHERE status::text NOT IN ('pending', 'processing', 'shipped', 'cancelled', 'returned')",
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_scd2_valid_intervals",
                "business_rule",
                "silver.product_categories",
                """
                SELECT (
                    (SELECT COUNT(*) FROM silver.customer_profiles WHERE valid_to_utc IS NOT NULL AND valid_to_utc < valid_from_utc) +
                    (SELECT COUNT(*) FROM silver.customer_addresses WHERE valid_to_utc IS NOT NULL AND valid_to_utc < valid_from_utc) +
                    (SELECT COUNT(*) FROM silver.product_categories WHERE valid_to_utc IS NOT NULL AND valid_to_utc < valid_from_utc)
                )
                """,
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_orders_fk_integrity",
                "referential_integrity",
                "silver.orders",
                """
                SELECT COUNT(*)
                FROM silver.orders o
                LEFT JOIN silver.customers c ON c.customer_id = o.customer_id
                LEFT JOIN silver.sales_channels sc ON sc.channel_id = o.sales_channel
                LEFT JOIN silver.stores s ON s.store_id = o.store_id
                WHERE c.customer_id IS NULL
                   OR sc.channel_id IS NULL
                   OR (o.store_id IS NOT NULL AND s.store_id IS NULL)
                """,
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_order_items_fk_integrity",
                "referential_integrity",
                "silver.order_items",
                """
                SELECT COUNT(*)
                FROM silver.order_items oi
                LEFT JOIN silver.orders o ON o.order_id = oi.order_id
                LEFT JOIN silver.products p ON p.product_id = oi.product_id
                WHERE o.order_id IS NULL OR p.product_id IS NULL
                """,
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_events_fk_orders_integrity",
                "referential_integrity",
                "silver.payment_events",
                """
                SELECT (
                    (SELECT COUNT(*) FROM silver.payment_events pe LEFT JOIN silver.orders o ON o.order_id = pe.order_id WHERE o.order_id IS NULL) +
                    (SELECT COUNT(*) FROM silver.refund_events re LEFT JOIN silver.orders o ON o.order_id = re.order_id WHERE o.order_id IS NULL) +
                    (SELECT COUNT(*) FROM silver.return_events rte LEFT JOIN silver.orders o ON o.order_id = rte.order_id WHERE o.order_id IS NULL)
                )
                """,
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_orders_bronze_reconciliation",
                "reconciliation",
                "silver.orders",
                """
                SELECT ABS(
                    (SELECT COUNT(*) FROM bronze.raw_orders WHERE pipeline_run_id = (SELECT pipeline_run_id FROM silver.orders LIMIT 1))
                    - (
                        (SELECT COUNT(*) FROM silver.orders) +
                        (SELECT COUNT(*) FROM silver.rejected_records WHERE source_table = 'silver.orders' AND pipeline_run_id = (SELECT pipeline_run_id FROM silver.orders LIMIT 1))
                    )
                )
                """,
                "0",
                lambda val: int(val) == 0,
            ),
            (
                "silver_rejected_records_logged",
                "business_rule",
                "silver.rejected_records",
                "SELECT COUNT(*) FROM silver.rejected_records WHERE rejection_reason IS NOT NULL AND rejection_reason <> ''",
                "> 0",
                lambda val: int(val) > 0,
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
                    "layer": "silver",
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
            print(f"[QualityGate:Silver] {check_name} ({target_table}): {status_label} (actual={actual_val}, expected={expected_str})")

        conn.commit()
        return all_passed
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
    rid = run_silver()
    run_quality_gate(rid)
