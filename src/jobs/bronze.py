"""Bronze jobs — Auto Loader ingestion (CE: trigger(availableNow=True))."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.config import get_config
from src.utils.engine import get_spark
from src.utils.logger import get_logger

log = get_logger("jobs.bronze")


def ingest_watch_events(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    land = f"{cfg.landing_root}/watch_events" if cfg.landing_root.startswith("/Volumes") else "/dbfs/mnt/landing/watch_events"
    chk = f"{cfg.checkpoint_root}/bronze_watch_events"
    tbl = f"`{cfg.catalog_name}`.{cfg.bronze_schema}.watch_events" if cfg.catalog_name else "bronze.watch_events"
    # Hive fallback when UC not enabled — init_schema.sql covers both
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg.catalog_name}`.{cfg.bronze_schema}")
    except Exception:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.bronze_schema}")
    log.info("Bronze watch_events: %s -> %s", land, tbl)
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", chk + "/schema")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(land)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    q = df.writeStream.format("delta").option("checkpointLocation", chk).option("mergeSchema", "true").trigger(availableNow=True).toTable(tbl)
    q.awaitTermination()
    log.info("Bronze watch_events done")


def ingest_subscriptions_cdc(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    land = "/dbfs/mnt/landing/subscriptions_cdc"
    chk = "/dbfs/mnt/checkpoints/bronze_subscriptions_cdc"
    tbl = f"`{cfg.catalog_name}`.{cfg.bronze_schema}.subscriptions_cdc" if cfg.catalog_name else "bronze.subscriptions_cdc"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.bronze_schema}")
    (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", chk + "/schema").load(land).withColumn("_ingested_at", F.current_timestamp()).writeStream.format("delta").option("checkpointLocation", chk).option("mergeSchema", "true").trigger(availableNow=True).toTable(tbl)).awaitTermination()


def ingest_content_catalog(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    land = "/dbfs/mnt/landing/content_catalog"
    tbl = f"`{cfg.catalog_name}`.{cfg.bronze_schema}.content_catalog"
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg.catalog_name}`.{cfg.bronze_schema}")
    except Exception:
        pass
    df = spark.read.option("mergeSchema", "true").json(land).withColumn("_ingested_at", F.current_timestamp())
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(tbl)


def ingest_billing(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    land = "/dbfs/mnt/landing/billing"
    chk = "/dbfs/mnt/checkpoints/bronze_billing"
    tbl = f"`{cfg.catalog_name}`.{cfg.bronze_schema}.billing_transactions"
    (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaLocation", chk + "/schema").load(land).withColumn("_ingested_at", F.current_timestamp()).writeStream.format("delta").option("checkpointLocation", chk).option("mergeSchema", "true").trigger(availableNow=True).toTable(tbl)).awaitTermination()


def run(spark=None, what: str = "all"):
    """Entry-point for Databricks Job task."""
    m = {
        "watch_events": ingest_watch_events,
        "subscriptions": ingest_subscriptions_cdc,
        "content": ingest_content_catalog,
        "billing": ingest_billing,
    }
    if what == "all":
        for fn in m.values():
            fn(spark)
    elif what in m:
        m[what](spark)
    else:
        raise ValueError(f"unknown bronze target: {what}")


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--what", default="all", choices=["all", "watch_events", "subscriptions", "content", "billing"])
    a = p.parse_args()
    run(what=a.what)
