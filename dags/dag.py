"""Airflow DAG for the Omnichannel Retail NL-to-SQL Analytics ETL pipeline.

Pipeline stages:
    validate_raw_data
        >> [create_bronze_schema, create_silver_schema, create_gold_schema, create_ops_schema]
        >> build_bronze_layer
        >> build_silver_layer
        >> quality_gate
        >> build_gold_layer
        >> validate_gold
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # Allows importing/testing DAG task callables outside an Airflow container

    class DAG:  # type: ignore[no-redef]
        def __init__(self, dag_id: str, **kwargs) -> None:
            self.dag_id = dag_id
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb) -> None:
            return None

    class PythonOperator:  # type: ignore[no-redef]
        def __init__(self, task_id: str, python_callable, **kwargs) -> None:
            self.task_id = task_id
            self.python_callable = python_callable
            self.kwargs = kwargs

        def __rshift__(self, other):
            return other

        def __rrshift__(self, other):
            return self

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.bronze.build_bronze import (  # noqa: E402
    BRONZE_SOURCES,
    _get_connection,
    _resolve_raw_dir,
    _resolve_source_file,
    run_bronze,
    validate_raw_manifest,
)
from pipelines.gold.build_gold import (  # noqa: E402
    _ensure_gold_schema,
    run_gold,
    validate_gold,
)
from pipelines.silver.build_silver import (  # noqa: E402
    run_quality_gate,
    run_silver,
)


def _resolve_run_id(context: dict) -> str:
    """Resolve a deterministic pipeline_run_id shared across all tasks in a DAG run."""
    ti = context.get("ti")
    if ti:
        pulled = ti.xcom_pull(task_ids="validate_raw_data", key="pipeline_run_id")
        if pulled:
            return str(pulled)
    dag_run = context.get("dag_run")
    if dag_run and getattr(dag_run, "run_id", None):
        return str(dag_run.run_id)
    return f"airflow_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"


def _on_task_failure(context: dict) -> None:
    """Record FAILED status in ops.pipeline_runs if any task fails."""
    run_id = _resolve_run_id(context)
    exc = context.get("exception")
    err_msg = str(exc) if exc else "Upstream task failed in omnichannel_retail_pipeline"
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()
    try:
        ops_sql_path = PROJECT_ROOT / "database" / "schemas" / "ops.sql"
        if ops_sql_path.exists():
            cur.execute(ops_sql_path.read_text(encoding="utf-8"))
        cur.execute(
            """
            INSERT INTO ops.pipeline_runs (run_id, started_at, finished_at, status, error_message)
            VALUES (%s, NOW(), NOW(), 'FAILED', %s)
            ON CONFLICT (run_id) DO UPDATE SET
                finished_at = NOW(),
                status = 'FAILED',
                error_message = EXCLUDED.error_message;
            """,
            (run_id, err_msg),
        )
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def validate_raw_data_task(**context) -> str:
    """Stage 1: Validate manifest.json and verify all 19 raw data files exist."""
    run_id = _resolve_run_id(context)
    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="pipeline_run_id", value=run_id)

    raw_dir = _resolve_raw_dir()
    manifest_checksum = validate_raw_manifest(raw_dir)

    for source in BRONZE_SOURCES:
        rel_path = source[0] if isinstance(source, tuple) else source["rel_path"]
        fpath = _resolve_source_file(raw_dir, rel_path)
        if not fpath.exists():
            raise FileNotFoundError(f"Required raw source file missing: {fpath}")

    if ti:
        ti.xcom_push(key="manifest_checksum", value=manifest_checksum)

    print(
        f"[validate_raw_data] Validated {len(BRONZE_SOURCES)} raw files in {raw_dir} "
        f"(run_id={run_id}, manifest_checksum={manifest_checksum})"
    )
    return run_id


def create_ops_schema_task(**context) -> None:
    """Initialize ops schema and register the RUNNING pipeline run."""
    run_id = _resolve_run_id(context)
    ti = context.get("ti")
    manifest_checksum = (
        ti.xcom_pull(task_ids="validate_raw_data", key="manifest_checksum") if ti else None
    )
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(20261101);")
        sql_path = PROJECT_ROOT / "database" / "schemas" / "ops.sql"
        if sql_path.exists():
            cur.execute(sql_path.read_text(encoding="utf-8"))
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
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def create_bronze_schema_task(**context) -> None:
    """Initialize bronze schema idempotently."""
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(20261101);")
        sql_path = PROJECT_ROOT / "database" / "schemas" / "bronze.sql"
        if sql_path.exists():
            cur.execute(sql_path.read_text(encoding="utf-8"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def create_silver_schema_task(**context) -> None:
    """Initialize silver schema idempotently (ensuring bronze.raw_ingestion exists first for FK)."""
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(20261101);")
        bronze_path = PROJECT_ROOT / "database" / "schemas" / "bronze.sql"
        if bronze_path.exists():
            cur.execute(bronze_path.read_text(encoding="utf-8"))
        silver_path = PROJECT_ROOT / "database" / "schemas" / "silver.sql"
        if silver_path.exists():
            cur.execute(silver_path.read_text(encoding="utf-8"))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def create_gold_schema_task(**context) -> None:
    """Initialize gold schema idempotently."""
    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(20261101);")
        gold_path = PROJECT_ROOT / "database" / "schemas" / "gold.sql"
        if gold_path.exists():
            cur.execute(gold_path.read_text(encoding="utf-8"))
        _ensure_gold_schema(cur)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


def build_bronze_layer_task(**context) -> str:
    """Stage 3: Load all 19 raw files into Bronze tables."""
    run_id = _resolve_run_id(context)
    return run_bronze(pipeline_run_id=run_id)


def build_silver_layer_task(**context) -> str:
    """Stage 4: Build typed, deduplicated, and validated Silver tables from Bronze."""
    run_id = _resolve_run_id(context)
    return run_silver(pipeline_run_id=run_id)


def quality_gate_task(**context) -> bool:
    """Stage 5: Execute Silver quality gate checks and halt if any check fails."""
    run_id = _resolve_run_id(context)
    passed = run_quality_gate(pipeline_run_id=run_id)
    if not passed:
        raise RuntimeError(f"Silver quality gate failed for run_id={run_id}")
    return passed


def build_gold_layer_task(**context) -> str:
    """Stage 6: Build all 5 Gold contract tables from Silver."""
    run_id = _resolve_run_id(context)
    return run_gold(pipeline_run_id=run_id)


def validate_gold_task(**context) -> bool:
    """Stage 7: Validate Gold tables, cross-table reconciliation, and mark pipeline run SUCCESS."""
    run_id = _resolve_run_id(context)
    passed = validate_gold(pipeline_run_id=run_id)
    if not passed:
        raise RuntimeError(f"Gold validation failed for run_id={run_id}")

    conn, driver = _get_connection()
    cur = conn._connection.cursor() if driver == "pg8000" else conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ops.pipeline_runs
            SET
                finished_at = NOW(),
                status = 'SUCCESS',
                error_message = NULL,
                bronze_record_count = (
                    SELECT COALESCE(SUM(records_loaded), 0)
                    FROM ops.layer_load_stats
                    WHERE pipeline_run_id = %s AND layer = 'bronze'
                ),
                silver_record_count = (
                    SELECT COALESCE(SUM(records_loaded), 0)
                    FROM ops.layer_load_stats
                    WHERE pipeline_run_id = %s AND layer = 'silver'
                ),
                gold_record_count = (
                    SELECT COALESCE(SUM(records_loaded), 0)
                    FROM ops.layer_load_stats
                    WHERE pipeline_run_id = %s AND layer = 'gold'
                ),
                rejected_record_count = (
                    SELECT COUNT(*) FROM silver.rejected_records
                )
            WHERE run_id = %s;
            """,
            (run_id, run_id, run_id, run_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    return passed


default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": _on_task_failure,
}

with DAG(
    dag_id="omnichannel_retail_pipeline",
    default_args=default_args,
    description="End-to-end Bronze, Silver, Quality Gate, and Gold ETL pipeline for Omnichannel Retail",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["omnichannel", "retail", "etl", "bronze", "silver", "gold"],
) as dag:
    validate_raw_data = PythonOperator(
        task_id="validate_raw_data",
        python_callable=validate_raw_data_task,
    )

    create_bronze_schema = PythonOperator(
        task_id="create_bronze_schema",
        python_callable=create_bronze_schema_task,
    )

    create_silver_schema = PythonOperator(
        task_id="create_silver_schema",
        python_callable=create_silver_schema_task,
    )

    create_gold_schema = PythonOperator(
        task_id="create_gold_schema",
        python_callable=create_gold_schema_task,
    )

    create_ops_schema = PythonOperator(
        task_id="create_ops_schema",
        python_callable=create_ops_schema_task,
    )

    build_bronze_layer = PythonOperator(
        task_id="build_bronze_layer",
        python_callable=build_bronze_layer_task,
    )

    build_silver_layer = PythonOperator(
        task_id="build_silver_layer",
        python_callable=build_silver_layer_task,
    )

    quality_gate = PythonOperator(
        task_id="quality_gate",
        python_callable=quality_gate_task,
    )

    build_gold_layer = PythonOperator(
        task_id="build_gold_layer",
        python_callable=build_gold_layer_task,
    )

    validate_gold_op = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold_task,
    )

    schema_tasks = [
        create_bronze_schema,
        create_silver_schema,
        create_gold_schema,
        create_ops_schema,
    ]

    validate_raw_data >> schema_tasks >> build_bronze_layer
    build_bronze_layer >> build_silver_layer >> quality_gate >> build_gold_layer >> validate_gold_op
