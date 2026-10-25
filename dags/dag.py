# -*- coding: utf-8 -*-
"""Airflow DAG orchestrating the end-to-end Bronze -> Silver -> Gold ETL pipeline.

Stages enforced in strict order:
1. ``validate_manifest`` – validate ``data/raw/manifest.json`` & verify SHA-256 file checksums
2. ``init_schemas`` – initialize ``ops``, ``bronze``, ``silver``, and ``gold`` schemas idempotently
3. ``load_bronze`` – ingest immutable raw payloads into ``bronze.raw_*`` tables
4. ``build_silver`` – normalize, deduplicate, resolve SCD2 & events, and log rejected records
5. ``quality_gate`` – verify Silver uniqueness, referential integrity, business rules, and reconciliation
6. ``build_gold`` – materialize ``order_360``, ``customer_daily``, ``product_daily``,
   ``channel_campaign_daily``, and ``executive_kpis_daily`` without fact multiplication
7. ``validate_gold`` – verify Gold grain, formulas, and cross-table reconciliations, then record run metadata

All tasks share the same ``pipeline_run_id`` via XCom and write operational metadata to ``ops.*``.
If ``quality_gate`` fails, ``build_gold`` and ``validate_gold`` are skipped/aborted and the run is
recorded as ``FAILED`` in ``ops.pipeline_runs``.
"""

from __future__ import annotations

import os
import sys
import uuid
from datetime import datetime
from pathlib import Path

# Ensure project root is on sys.path so ``pipelines.*`` imports resolve inside Airflow workers
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402

from pipelines.bronze.build_bronze import (  # noqa: E402
     BRONZE_SOURCES,
    _get_connection,
    _resolve_raw_dir,
    run_bronze,
    validate_raw_manifest,
)
from pipelines.gold.build_gold import (  # noqa: E402
    _ensure_gold_schema,
    run_gold,
    validate_gold as run_validate_gold,
)
from pipelines.silver.build_silver import (  # noqa: E402
    run_quality_gate,
    run_silver,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _new_run_id() -> str:
    """Generate a unique run identifier shared across all pipeline stages."""
    return f"run_{uuid.uuid4().hex[:12]}"


def _resolve_run_id(context: dict) -> str:
    """Retrieve shared pipeline_run_id from XCom or dag_run.conf."""
    ti = context.get("ti")
    if ti:
        run_id = ti.xcom_pull(task_ids="validate_manifest", key="pipeline_run_id")
        if run_id:
            return str(run_id)
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "conf", None) and dag_run.conf.get("pipeline_run_id"):
        return str(dag_run.conf["pipeline_run_id"])
    return _new_run_id()


# ---------------------------------------------------------------------------
# Task Callables
# ---------------------------------------------------------------------------
def validate_manifest_task(**context) -> str:
    """Stage 1: Validate raw data files and manifest checksums."""
    dag_run = context.get("dag_run")
    conf_run_id = (
        dag_run.conf.get("pipeline_run_id")
        if dag_run and getattr(dag_run, "conf", None)
        else None
    )
    run_id = str(conf_run_id) if conf_run_id else _new_run_id()

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="pipeline_run_id", value=run_id)

    raw_dir = _resolve_raw_dir()
    manifest_checksum = validate_raw_manifest(raw_dir)
    for rel_path, _tbl, _id_field in BRONZE_SOURCES:
        fpath = raw_dir / rel_path
        if not fpath.exists():
            raise FileNotFoundError(f"Required raw source file missing: {fpath}")

    if ti:
        ti.xcom_push(key="manifest_checksum", value=manifest_checksum)

    print(
        f"[validate_manifest] Validated {len(BRONZE_SOURCES)} raw files in {raw_dir} "
        f"(run_id={run_id}, manifest_checksum={manifest_checksum})"
    )
    return run_id


def init_schemas_task(**context) -> None:
    """Stage 2: Initialize PostgreSQL schemas (ops, bronze, silver, gold) idempotently."""
    run_id = _resolve_run_id(context)
    ti = context.get("ti")
    manifest_checksum = (
        ti.xcom_pull(task_ids="validate_manifest", key="manifest_checksum") if ti else None
    )

    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()

    try:
        schemas_dir = PROJECT_ROOT / "database" / "schemas"
        for sql_name in ("ops.sql", "bronze.sql", "silver.sql", "gold.sql"):
            sql_path = schemas_dir / sql_name
            if sql_path.exists():
                cur.execute(sql_path.read_text(encoding="utf-8"))

        # Always ensure gold contract tables exist even if gold.sql is omitted from submission package
        _ensure_gold_schema(cur)

        cur.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, started_at, status, manifest_checksum, error_message)
            VALUES (%s, NOW(), 'RUNNING', %s, NULL)
            ON CONFLICT (run_id) DO UPDATE SET
                started_at = NOW(),
                status = 'RUNNING',
                manifest_checksum = COALESCE(EXCLUDED.manifest_checksum, ops.pipeline_runs.manifest_checksum),
                error_message = NULL;
            """,
            (run_id, manifest_checksum),
        )
        conn.commit()
        print(f"[init_schemas] Initialized schemas ops, bronze, silver, gold (run_id={run_id})")
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()


def load_bronze_task(**context) -> str:
    """Stage 3: Load raw source files into Bronze tables."""
    run_id = _resolve_run_id(context)
    return run_bronze(pipeline_run_id=run_id)


def build_silver_task(**context) -> str:
    """Stage 4: Transform Bronze JSONB records into normalized Silver tables."""
    run_id = _resolve_run_id(context)
    return run_silver(pipeline_run_id=run_id)


def quality_gate_task(**context) -> bool:
    """Stage 5: Execute Silver quality checks; abort pipeline if any check fails."""
    run_id = _resolve_run_id(context)
    passed = run_quality_gate(pipeline_run_id=run_id)
    if not passed:
        raise RuntimeError(f"Quality gate failed for run_id={run_id} – aborting downstream Gold build")
    return True


def build_gold_task(**context) -> str:
    """Stage 6: Materialize analytical Gold tables from Silver."""
    run_id = _resolve_run_id(context)
    return run_gold(pipeline_run_id=run_id)


def validate_gold_task(**context) -> bool:
    """Stage 7: Validate Gold tables and mark pipeline run as SUCCESS in ops.pipeline_runs."""
    run_id = _resolve_run_id(context)
    passed = run_validate_gold(pipeline_run_id=run_id)
    if not passed:
        raise RuntimeError(f"Gold validation failed for run_id={run_id}")

    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ops.pipeline_runs
            SET finished_at = NOW(),
                status = 'SUCCESS',
                error_message = NULL
            WHERE run_id = %s;
            """,
            (run_id,),
        )
        conn.commit()
        print(f"[validate_gold] Gold validation passed and pipeline marked SUCCESS (run_id={run_id})")
    except Exception:
        conn.rollback()
        raise
    finally:
        try:
            cur.close()
        except Exception:
            pass
        conn.close()
    return True


def record_failure_task(**context) -> None:
    """Record FAILED status in ops.pipeline_runs if any upstream task failed."""
    run_id = _resolve_run_id(context)
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, finished_at, status, error_message)
            VALUES (%s, NOW(), 'FAILED', 'One or more upstream DAG tasks failed')
            ON CONFLICT (run_id) DO UPDATE SET
                finished_at = NOW(),
                status = 'FAILED',
                error_message = COALESCE(ops.pipeline_runs.error_message, 'One or more upstream DAG tasks failed');
            """,
            (run_id,),
        )
        conn.commit()
        print(f"[record_failure] Pipeline run marked FAILED (run_id={run_id})")
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
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="omnichannel_retail_etl_pipeline",
    description="End-to-end Bronze, Silver, Quality Gate, and Gold ETL pipeline for Omnichannel Retail",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "bronze", "silver", "gold", "omnichannel"],
) as dag:
    validate_manifest = PythonOperator(
        task_id="validate_manifest",
        python_callable=validate_manifest_task,
    )

    init_schemas = PythonOperator(
        task_id="init_schemas",
        python_callable=init_schemas_task,
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=load_bronze_task,
    )

    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=build_silver_task,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_task,
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=build_gold_task,
    )

    validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold_task,
    )

    record_failure = PythonOperator(
        task_id="record_failure",
        python_callable=record_failure_task,
        trigger_rule="one_failed",
    )

    # Linear stage progression matching README section 8
    (
        validate_manifest
        >> init_schemas
        >> load_bronze
        >> build_silver
        >> quality_gate
        >> build_gold
        >> validate_gold
    )

    # Failure handler attached to all stages
    [
        validate_manifest,
        init_schemas,
        load_bronze,
        build_silver,
        quality_gate,
        build_gold,
        validate_gold,
    ] >> record_failure
