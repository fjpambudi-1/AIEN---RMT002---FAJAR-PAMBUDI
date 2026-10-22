def run_bronze(pipeline_run_id: str):
    """Load raw source files into Bronze tables.

    This stub reads CSV/JSON files from the `data/raw` directory and inserts
    them into the corresponding `bronze.raw_*` tables.  The implementation
    should use `INSERT ... ON CONFLICT DO NOTHING` (or upserts) and record the
    `pipeline_run_id` for lineage.
    """
    # TODO: implement actual ingestion logic.
    pass
