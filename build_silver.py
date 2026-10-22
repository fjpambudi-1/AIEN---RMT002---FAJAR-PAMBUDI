def run_silver(pipeline_run_id: str):
    """Transform Bronze → Silver.

    This stub extracts data from the Bronze tables, performs:
    * deterministic deduplication (using `source_row_id`)
    * foreign‑key validation against other Silver tables
    * timestamp normalisation to UTC
    * rejection handling – rejected rows should be written to
      `silver.rejected_records` with a reason.
    The function should insert the clean rows into the appropriate Silver
    tables while propagating the same `pipeline_run_id` for lineage.
    """
    # TODO: implement actual transformation logic.
    pass
