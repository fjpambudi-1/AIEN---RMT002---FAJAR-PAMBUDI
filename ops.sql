-- =============================================================================
-- Schema: ops
-- Purpose: Pipeline metadata, run tracking, and data quality checks
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS ops;

-- -----------------------------------------------------------------------------
-- ops.pipeline_runs
-- Tracks each execution of the data pipeline end-to-end.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id              TEXT            PRIMARY KEY,
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at         TIMESTAMPTZ,
    status              TEXT            NOT NULL DEFAULT 'RUNNING'
                                        CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    error_message       TEXT,
    manifest_checksum   TEXT,
    bronze_record_count INTEGER,
    silver_record_count INTEGER,
    gold_record_count   INTEGER,
    rejected_record_count INTEGER
);

-- -----------------------------------------------------------------------------
-- ops.quality_check_results
-- Stores the outcome of every data-quality check executed during a pipeline run.
-- Categories: uniqueness, reconciliation, business_rule, referential_integrity
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.quality_check_results (
    check_id            BIGSERIAL       PRIMARY KEY,
    pipeline_run_id     TEXT            NOT NULL,
    check_name          TEXT            NOT NULL,
    check_category      TEXT            NOT NULL,
    target_table        TEXT            NOT NULL,
    check_query         TEXT,
    expected_result     TEXT,
    actual_result       TEXT,
    passed              BOOLEAN         NOT NULL,
    checked_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    details             JSONB
);

-- -----------------------------------------------------------------------------
-- ops.layer_load_stats
-- Per-table load statistics for each pipeline layer (bronze / silver / gold).
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ops.layer_load_stats (
    stat_id             BIGSERIAL       PRIMARY KEY,
    pipeline_run_id     TEXT            NOT NULL,
    layer               TEXT            NOT NULL
                                        CHECK (layer IN ('bronze', 'silver', 'gold')),
    table_name          TEXT            NOT NULL,
    records_loaded      INTEGER         NOT NULL DEFAULT 0,
    records_rejected    INTEGER         NOT NULL DEFAULT 0,
    loaded_at           TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
