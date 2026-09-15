"""Pipeline orchestrator — Bronze → Silver → Gold, with audit."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
)
from rich.table import Table

from src.config import get_config
from src.jobs import bronze, gold, silver
from src.utils.console import console, quiet_fds
from src.utils.engine import ensure_schemas, get_spark, table_fqn
from src.utils.logger import get_logger

log = get_logger("jobs.pipeline")

SILVER_TABLES = [
    "watch_events",
    "subscriptions_scd2",
    "billing",
    "devices_scd2",
    "profiles",
    "promotions",
    "promotion_redemptions",
    "support_tickets",
    "cdn_stream_sessions",
    "content_ratings",
]
GOLD_TABLES = [
    "daily_active_users",
    "weekly_active_users",
    "watch_time_by_genre",
    "churn_signals",
    "mrr_trend",
    "qoe_by_device",
    "promo_effectiveness",
    "support_ticket_summary",
    "content_engagement",
]


def _summary_table(spark, cfg) -> Table:
    t = Table(title="Pipeline output", title_style="bold", header_style="bold")
    t.add_column("layer", style="dim")
    t.add_column("table")
    t.add_column("rows", justify="right")
    for schema, tables, layer in (
        (cfg.silver_schema, SILVER_TABLES, "silver"),
        (cfg.gold_schema, GOLD_TABLES, "gold"),
    ):
        for tbl in tables:
            try:
                n = str(spark.table(table_fqn(schema, tbl)).count())
            except Exception:
                n = "—"
            t.add_row(layer, tbl, n)
    return t


def run(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    ensure_schemas(spark)
    run_id = str(uuid.uuid4())
    target = cfg.catalog_name if not cfg.is_local else "local .spark/warehouse"
    log.info("pipeline start run_id=%s target=%s env=%s", run_id, target, cfg.env)

    progress = Progress(
        SpinnerColumn("dots", style="dim"),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None, pulse_style="dim"),
        MofNCompleteColumn(),
        console=console,
        transient=False,
    )
    with progress:
        layers = (
            ("bronze", bronze.ORDER, bronze.run),
            ("silver", silver.ORDER, silver.run),
            ("gold", list(gold.ORDER), gold.run),
        )
        tasks = {}
        for layer, order, run_layer in layers:
            tasks[layer] = progress.add_task(f"[bold]{layer}", total=len(order))
            for what in order:
                run_layer(spark, what)
                progress.advance(tasks[layer])

    # pipeline_runs audit
    try:
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType(
            [
                StructField("run_id", StringType()),
                StructField("pipeline_name", StringType()),
                StructField("layer", StringType()),
                StructField("start_time", TimestampType()),
                StructField("end_time", TimestampType()),
                StructField("status", StringType()),
                StructField("records_processed", LongType()),
                StructField("error_message", StringType()),
                StructField("run_by", StringType()),
                StructField("cluster_id", StringType()),
            ]
        )
        spark.createDataFrame(
            [
                (
                    run_id,
                    "streamflix-pipeline",
                    "gold",
                    datetime.now(UTC),
                    datetime.now(UTC),
                    "SUCCESS",
                    0,
                    None,
                    "jobs.pipeline",
                    None,
                )
            ],
            schema,
        ).write.format("delta").mode("append").saveAsTable(
            table_fqn(cfg.init_schema, "pipeline_runs")
        )
    except Exception as e:
        log.warning("pipeline_runs audit failed: %s", e)

    console.print(_summary_table(spark, cfg))
    log.info("pipeline done run_id=%s", run_id)

    # Graceful JVM shutdown — and capture the Windows taskkill chatter
    if cfg.is_local:
        with quiet_fds():
            try:
                spark.stop()
            except Exception:
                pass


if __name__ == "__main__":
    run()
