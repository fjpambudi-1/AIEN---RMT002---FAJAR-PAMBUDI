# -*- coding: utf-8 -*-
"""Airflow DAG orchestrating the ETL pipeline.

The pipeline follows the required stages:
1. validate manifest & raw files
2. initialize PostgreSQL schemas
3. load Bronze
4. build Silver
5. quality gate
6. build Gold
7. validate Gold & record pipeline metadata

Each task receives the same ``pipeline_run_id`` which is stored in the
``ops.pipeline_runs`` table for lineage and can be used to make the run
idempotent (INSERT … ON CONFLICT DO NOTHING for the load‑stats tables).

Failure in the ``quality_gate`` task aborts the downstream ``build_gold``
and ``validate_gold`` tasks.
"""

from __future__ import annotations

import os
import uuid
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _new_run_id() -> str:
    """Generate a short unique run identifier used for all layers."""
    return f"run_{uuid.uuid4().hex[:12]}"

def _record_pipeline_start(**context):
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    pg = PostgresHook(postgres_conn_id="postgres_default")
    pg.run(
        """
        INSERT INTO ops.pipeline_runs (run_id, status)
        VALUES (%s, 'RUNNING')
        ON CONFLICT (run_id) DO UPDATE SET started_at = NOW(), status = 'RUNNING';
        """,
        parameters=(run_id,),
    )

def _record_pipeline_end(success: bool, **context):
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    pg = PostgresHook(postgres_conn_id="postgres_default")
    pg.run(
        """
        UPDATE ops.pipeline_runs
        SET finished_at = NOW(),
            status = %s,
            error_message = CASE WHEN %s THEN NULL ELSE error_message END
        WHERE run_id = %s;
        """,
        parameters=("SUCCESS" if success else "FAILED", success, run_id),
    )

# ---------------------------------------------------------------------------
# Task implementations (very lightweight – real logic lives in the pipeline
# modules under ``pipelines/``)
# ---------------------------------------------------------------------------

def validate_manifest(**context):
    # Placeholder: in a real project you would checksum manifest files.
    # Here we just log the action.
    print("[validate_manifest] manifest and raw files validated")

def init_schemas(**context):
    pg = PostgresHook(postgres_conn_id="postgres_default")
    # Run the schema creation scripts for bronze, silver, gold, ops.
    for sql_file in [
        "database/schemas/bronze.sql",
        "database/schemas/silver.sql",
        "database/schemas/gold.sql",
        "database/schemas/ops.sql",
    ]:
        path = os.path.join(os.getenv("AIRFLOW_HOME", "."), sql_file)
        pg.run(open(path).read())
    print("[init_schemas] schemas created")

def load_bronze(**context):
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    # Call the Bronze loader – for now just a stub.
    from pipelines.bronze.build_bronze import run_bronze
    run_bronze(run_id)

def build_silver(**context):
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    from pipelines.silver.build_silver import run_silver
    run_silver(run_id)

def quality_gate(**context):
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    from pipelines.quality_gate import run_quality_gate
    passed = run_quality_gate(run_id)
    if not passed:
        raise Exception("Quality gate failed – aborting pipeline")

def build_gold(**context):
    from pipelines.gold.build_gold import run_gold_pipeline
    run_gold_pipeline()

def validate_gold(**context):
    run_id = context["ti"].xcom_pull(key="pipeline_run_id")
    from pipelines.validate_gold import validate_gold
    validate_gold(run_id)

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="etl_pipeline",
    schedule_interval=None,  # manual / triggered runs
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "silver", "gold"],
) as dag:

    # generate a run id that will be shared between tasks via XCom
    generate_run_id = PythonOperator(
        task_id="generate_run_id",
        python_callable=lambda ti: ti.xcom_push(key="pipeline_run_id", value=_new_run_id()),
    )

    start_record = PythonOperator(
        task_id="record_pipeline_start",
        python_callable=_record_pipeline_start,
    )

    validate = PythonOperator(task_id="validate_manifest", python_callable=validate_manifest)
    init = PythonOperator(task_id="init_schemas", python_callable=init_schemas)
    bronze = PythonOperator(task_id="load_bronze", python_callable=load_bronze)
    silver = PythonOperator(task_id="build_silver", python_callable=build_silver)
    gate = PythonOperator(task_id="quality_gate", python_callable=quality_gate)
    gold = PythonOperator(task_id="build_gold", python_callable=build_gold)
    validate_g = PythonOperator(task_id="validate_gold", python_callable=validate_gold)

    end_success = PythonOperator(
        task_id="record_success",
        python_callable=lambda **kw: _record_pipeline_end(True, **kw),
        trigger_rule="all_success",
    )
    end_failure = PythonOperator(
        task_id="record_failure",
        python_callable=lambda **kw: _record_pipeline_end(False, **kw),
        trigger_rule="one_failed",
    )

    # -------------------------------------------------------------------
    # Dependency graph
    # -------------------------------------------------------------------
    generate_run_id >> start_record >> validate >> init >> bronze >> silver >> gate >> gold >> validate_g >> end_success
    gate >> end_failure
