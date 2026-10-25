# -*- coding: utf-8 -*-
"""Bronze pipeline: ingest raw source files from data/raw into Bronze PostgreSQL tables.

Responsibilities according to README:
1. Validate raw files against ``data/raw/manifest.json`` (SHA-256 checksums & existence).
2. Preserve immutable source evidence in ``bronze.raw_*`` tables as ``JSONB`` without
   modifying the original payload, performing business joins, or filtering out
   duplicates / invalid records.
3. Record lineage metadata for every row:
   - ``source_file`` (relative path under ``data/raw``)
   - ``source_row_num`` (1-based line/element index in the file)
   - ``source_row_id`` (deterministic source identifier)
   - ``raw_payload`` (original record as JSONB)
   - ``checksum`` (SHA-256 hex digest of canonical JSON payload)
   - ``ingested_at`` (ingestion UTC timestamp)
   - ``pipeline_run_id`` (shared run identifier for lineage)
4. Maintain idempotency on re-runs using ``INSERT ... ON CONFLICT DO NOTHING``.
5. Record per-table ingestion counts in ``ops.layer_load_stats`` and update
   ``ops.pipeline_runs``.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Configuration & Source Mapping
# ---------------------------------------------------------------------------
DB_URL = os.getenv(
    "DATABASE_URL",
    os.getenv("LOCAL_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres"),
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "raw"

# Mapping of (relative_file_path, bronze_table_name, primary_id_field)
BRONZE_SOURCES: list[tuple[str, str, str | None]] = [
    # Operational JSON files
    ("operational/customers.json", "bronze.raw_customers", "source_row_id"),
    ("operational/customer_profiles.json", "bronze.raw_customer_profiles", "source_row_id"),
    ("operational/customer_addresses.json", "bronze.raw_customer_addresses", "source_row_id"),
    ("operational/products.json", "bronze.raw_products", "source_row_id"),
    ("operational/product_categories.json", "bronze.raw_product_categories", "source_row_id"),
    ("operational/stores.json", "bronze.raw_stores", "source_row_id"),
    ("operational/sales_channels.json", "bronze.raw_sales_channels", "source_row_id"),
    ("operational/promotions.json", "bronze.raw_promotions", "source_row_id"),
    ("operational/orders.json", "bronze.raw_orders", "source_row_id"),
    ("operational/order_items.json", "bronze.raw_order_items", "source_row_id"),
    ("operational/order_promotions.json", "bronze.raw_order_promotions", "source_row_id"),
    # Event JSON files
    ("events/payment_events.json", "bronze.raw_payment_events", "event_id"),
    ("events/refund_events.json", "bronze.raw_refund_events", "event_id"),
    ("events/return_events.json", "bronze.raw_return_events", "event_id"),
    ("events/support_events.json", "bronze.raw_support_events", "event_id"),
    ("events/web_events.json", "bronze.raw_web_events", "event_id"),
    # Inventory CSV file
    ("inventory/inventory_snapshots.csv", "bronze.raw_inventory_snapshots", None),
    # Reference CSV & JSON files
    ("reference/campaign_spend.csv", "bronze.raw_campaign_spend", None),
    ("reference/city_reference.json", "bronze.raw_city_reference", "city_id"),
]


def _new_run_id() -> str:
    """Generate a unique run identifier if none was supplied."""
    return f"bronze_run_{uuid.uuid4().hex[:12]}"


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


def _resolve_raw_dir(raw_dir: str | Path | None = None) -> Path:
    """Resolve the raw data lake folder location across local and container environments."""
    if raw_dir is not None:
        return Path(raw_dir)
    env_dir = os.getenv("RAW_DATA_DIR")
    if env_dir and Path(env_dir).exists():
        return Path(env_dir)
    candidates = [
        DEFAULT_RAW_DIR,
        Path.cwd() / "data" / "raw",
        PROJECT_ROOT.parent / "Eksperimen" / "data" / "raw",
        PROJECT_ROOT.parent / "Milestone 1" / "Raw Files",
        Path("/opt/airflow/data/raw"),
        Path("/opt/airflow/data"),
    ]
    for candidate in candidates:
        if (candidate / "operational" / "orders.json").exists():
            return candidate
    return DEFAULT_RAW_DIR


def _compute_payload_checksum(record: dict[str, Any]) -> str:
    """Compute a deterministic SHA-256 checksum of the raw JSON payload."""
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _extract_source_row_id(
    record: dict[str, Any],
    rel_path: str,
    row_num: int,
    id_field: str | None,
    seen_ids: set[str],
) -> str:
    """Derive a deterministic source_row_id while preserving duplicate raw rows.

    Bronze tables must preserve all raw records (including duplicates) without
    data loss, while also supporting idempotent re-runs via the unique index on
    ``(source_row_id, pipeline_run_id)``. If the same identifier appears more
    than once within the same file, we suffix ``#row{row_num}`` on subsequent
    occurrences so that every raw row is retained in Bronze.
    """
    base_id: str | None = None

    if record.get("source_row_id"):
        base_id = str(record["source_row_id"])
    elif id_field and record.get(id_field):
        base_id = str(record[id_field])
    elif rel_path.endswith("inventory_snapshots.csv"):
        prod = record.get("product_id", "UNKNOWN")
        loc = record.get("location_id", "UNKNOWN")
        dt = record.get("snapshot_date", "UNKNOWN")
        base_id = f"INV-{prod}-{loc}-{dt}"
    elif rel_path.endswith("campaign_spend.csv"):
        dt = record.get("spend_date", "UNKNOWN")
        cmp_id = record.get("campaign_id", "UNKNOWN")
        ch = record.get("channel", "UNKNOWN")
        base_id = f"SPEND-{dt}-{cmp_id}-{ch}"
    else:
        stem = Path(rel_path).stem.upper()
        base_id = f"{stem}-ROW-{row_num:06d}"

    candidate = base_id
    if candidate in seen_ids:
        candidate = f"{base_id}#row{row_num}"
    seen_ids.add(candidate)
    return candidate


def _read_raw_records(file_path: Path) -> list[dict[str, Any]]:
    """Read raw records from a JSON or CSV file without altering payload values."""
    if not file_path.exists():
        raise FileNotFoundError(f"Raw source file not found: {file_path}")

    suffix = file_path.suffix.lower()
    if suffix == ".json":
        content = file_path.read_text(encoding="utf-8")
        data = json.loads(content)
        if not isinstance(data, list):
            raise ValueError(f"Expected JSON array in {file_path}, got {type(data).__name__}")
        return data
    if suffix == ".csv":
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return [dict(row) for row in reader]
    raise ValueError(f"Unsupported file extension '{suffix}' for {file_path}")


def validate_raw_manifest(raw_dir: Path) -> str | None:
    """Validate ``manifest.json`` and verify SHA-256 checksums of all listed files.

    Returns the SHA-256 checksum of ``manifest.json`` if present, else ``None``.
    """
    manifest_path = raw_dir / "manifest.json"
    if not manifest_path.exists():
        return None

    manifest_bytes = manifest_path.read_bytes()
    manifest_checksum = hashlib.sha256(manifest_bytes).hexdigest()
    manifest = json.loads(manifest_bytes.decode("utf-8"))

    files_map = manifest.get("files", {})
    for rel_path, expected_sha256 in files_map.items():
        target = raw_dir / rel_path
        if not target.exists():
            raise FileNotFoundError(f"File listed in manifest.json is missing: {target}")
        actual_sha256 = hashlib.sha256(target.read_bytes()).hexdigest()
        if actual_sha256 != expected_sha256:
            raise ValueError(
                f"Checksum mismatch for {rel_path}: expected {expected_sha256}, got {actual_sha256}"
            )
    return manifest_checksum


def _ensure_compat_raw_ingestion_table(cur) -> None:
    """Ensure ``bronze.raw_ingestion`` exists if referenced by Silver foreign keys."""
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


def _sync_compat_raw_ingestion(cur, table_name: str, pipeline_run_id: str) -> None:
    """Populate ``bronze.raw_ingestion`` with ``bronze_id`` keys for FK compatibility."""
    cur.execute(
        f"""
        INSERT INTO bronze.raw_ingestion (id, source_table, source_file, source_row_id, pipeline_run_id, ingested_at)
        SELECT bronze_id, %s, source_file, source_row_id, pipeline_run_id, ingested_at
        FROM {table_name}
        WHERE pipeline_run_id = %s
        ON CONFLICT (id) DO UPDATE SET
            pipeline_run_id = EXCLUDED.pipeline_run_id,
            ingested_at = EXCLUDED.ingested_at;
        """,
        (table_name, pipeline_run_id),
    )


def _record_bronze_stats(
    cur,
    pipeline_run_id: str,
    table_counts: dict[str, int],
    manifest_checksum: str | None,
) -> None:
    """Record Bronze ingestion metrics into ``ops.layer_load_stats`` and ``ops.pipeline_runs``."""
    total_loaded = sum(table_counts.values())

    # Ensure pipeline_runs row exists
    cur.execute(
        """
        INSERT INTO ops.pipeline_runs (run_id, status, manifest_checksum, bronze_record_count)
        VALUES (%s, 'RUNNING', %s, %s)
        ON CONFLICT (run_id) DO UPDATE
        SET manifest_checksum = COALESCE(EXCLUDED.manifest_checksum, ops.pipeline_runs.manifest_checksum),
            bronze_record_count = EXCLUDED.bronze_record_count;
        """,
        (pipeline_run_id, manifest_checksum, total_loaded),
    )

    # Clear previous stats for the same (pipeline_run_id, 'bronze') to stay idempotent
    cur.execute(
        """
        DELETE FROM ops.layer_load_stats
        WHERE pipeline_run_id = %s AND layer = 'bronze';
        """,
        (pipeline_run_id,),
    )

    for table_name, count in table_counts.items():
        cur.execute(
            """
            INSERT INTO ops.layer_load_stats (
                pipeline_run_id, layer, table_name, records_loaded, records_rejected
            )
            VALUES (%s, 'bronze', %s, %s, 0);
            """,
            (pipeline_run_id, table_name, count),
        )


def run_bronze(pipeline_run_id: str | None = None, raw_dir: str | Path | None = None) -> str:
    """Load raw source files into Bronze tables.

    Reads CSV/JSON files from the ``data/raw`` directory and inserts them into
    the corresponding ``bronze.raw_*`` tables. Uses
    ``INSERT ... ON CONFLICT (source_row_id, pipeline_run_id) DO NOTHING`` and
    records ``pipeline_run_id`` for lineage.
    """
    run_id = pipeline_run_id or _new_run_id()
    resolved_raw_dir = _resolve_raw_dir(raw_dir)
    manifest_checksum = validate_raw_manifest(resolved_raw_dir)
    now_utc = datetime.now(timezone.utc)

    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()

    table_counts: dict[str, int] = {}

    try:
        _ensure_compat_raw_ingestion_table(cur)

        for rel_path, table_name, id_field in BRONZE_SOURCES:
            file_path = resolved_raw_dir / rel_path
            raw_records = _read_raw_records(file_path)

            # Check already-ingested (source_row_id, checksum) pairs to prevent duplicating unchanged files across reruns
            cur.execute(
                f"SELECT source_row_id, checksum FROM {table_name} WHERE source_file = %s",
                (rel_path,),
            )
            existing_pairs = {(str(r[0]), str(r[1])) for r in cur.fetchall()}

            seen_ids: set[str] = set()
            rows_to_insert: list[tuple[Any, ...]] = []
            reused_count = 0

            for idx, record in enumerate(raw_records, start=1):
                source_row_id = _extract_source_row_id(
                    record=record,
                    rel_path=rel_path,
                    row_num=idx,
                    id_field=id_field,
                    seen_ids=seen_ids,
                )
                payload_json = json.dumps(record, ensure_ascii=False)
                checksum = _compute_payload_checksum(record)

                if (source_row_id, checksum) in existing_pairs:
                    reused_count += 1
                    continue

                rows_to_insert.append(
                    (
                        rel_path,
                        idx,
                        source_row_id,
                        payload_json,
                        checksum,
                        now_utc,
                        run_id,
                    )
                )

            if reused_count > 0:
                cur.execute(
                    f"""
                    UPDATE {table_name}
                    SET pipeline_run_id = %s,
                        ingested_at = %s
                    WHERE source_file = %s
                    """,
                    (run_id, now_utc, rel_path),
                )

            insert_sql = f"""
                INSERT INTO {table_name} (
                    source_file,
                    source_row_num,
                    source_row_id,
                    raw_payload,
                    checksum,
                    ingested_at,
                    pipeline_run_id
                )
                VALUES %s
                ON CONFLICT (source_row_id, pipeline_run_id) DO NOTHING
            """

            if rows_to_insert:
                if driver == "psycopg2":
                    from psycopg2.extras import execute_values

                    execute_values(
                        cur,
                        insert_sql,
                        rows_to_insert,
                        template="(%s, %s, %s, %s::jsonb, %s, %s, %s)",
                        page_size=2000,
                    )
                else:
                    row_sql = f"""
                        INSERT INTO {table_name} (
                            source_file,
                            source_row_num,
                            source_row_id,
                            raw_payload,
                            checksum,
                            ingested_at,
                            pipeline_run_id
                        )
                        VALUES (%s, %s, %s, CAST(%s AS JSONB), %s, %s, %s)
                        ON CONFLICT (source_row_id, pipeline_run_id) DO NOTHING
                    """
                    cur.executemany(row_sql, rows_to_insert)

            _sync_compat_raw_ingestion(cur, table_name, run_id)

            cur.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE pipeline_run_id = %s",
                (run_id,),
            )
            loaded_count = int(cur.fetchone()[0])
            table_counts[table_name] = loaded_count
            print(f"[Bronze] {table_name}: {loaded_count} records ingested from {rel_path}")

        _record_bronze_stats(cur, run_id, table_counts, manifest_checksum)
        conn.commit()
        print(f"Bronze pipeline completed successfully (run_id={run_id})")
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


if __name__ == "__main__":
    run_bronze()
