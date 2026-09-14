"""Pipeline orchestrator — Bronze → Silver → Gold, with audit."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from src.config import get_config
from src.jobs import bronze, gold, silver
from src.utils.engine import get_spark
from src.utils.logger import get_logger

log = get_logger("jobs.pipeline")


def run(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    run_id = str(uuid.uuid4())
    log.info("pipeline start run_id=%s catalog=%s env=%s", run_id, cfg.catalog_name, cfg.env)
    # Bronze
    bronze.run(spark, "all")
    # Silver
    silver.run(spark, "all")
    # Gold
    gold.run(spark, "all")
    # pipeline_runs audit
    try:
        spark.createDataFrame(
            [(run_id, "streamflix-pipeline", "gold", datetime.now(UTC), datetime.now(UTC), "SUCCESS", 0, None, "jobs.pipeline", None)],
            ["run_id", "pipeline_name", "layer", "start_time", "end_time", "status", "records_processed", "error_message", "run_by", "cluster_id"],
        ).write.format("delta").mode("append").saveAsTable(f"`{cfg.catalog_name}`.{cfg.init_schema}.pipeline_runs")
    except Exception as e:
        log.warning("pipeline_runs audit failed: %s", e)
    log.info("pipeline done run_id=%s", run_id)


if __name__ == "__main__":
    run()
