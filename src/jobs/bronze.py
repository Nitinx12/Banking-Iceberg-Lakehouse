"""Bronze jobs — Auto Loader ingestion on Databricks, batch JSON locally.

cloudFiles (Auto Loader) is a Databricks-only data source, so local runs read
the landing JSON directly and append/overwrite the Delta table — Silver's
MERGE-on-natural-key keeps the pipeline idempotent either way.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import functions as F

from src.config import get_config
from src.utils.engine import ensure_schema, get_spark, table_fqn
from src.utils.logger import get_logger

log = get_logger("jobs.bronze")

# cli name -> (landing dir, bronze table, ingest mode)
SOURCES: dict[str, tuple[str, str, str]] = {
    "watch_events": ("watch_events", "watch_events", "stream"),
    "subscriptions": ("subscriptions_cdc", "subscriptions_cdc", "stream"),
    "content": ("content_catalog", "content_catalog", "batch"),
    "billing": ("billing", "billing_transactions", "stream"),
    "devices": ("devices_cdc", "devices_cdc", "stream"),
    "profiles": ("profiles", "profiles", "batch"),
    "promotions": ("promotions", "promotions", "batch"),
    "redemptions": ("promotion_redemptions", "promotion_redemptions", "stream"),
    "tickets": ("support_tickets", "support_tickets", "stream"),
    "cdn_logs": ("cdn_stream_logs", "cdn_stream_logs", "stream"),
    "ratings": ("content_ratings", "content_ratings", "stream"),
}

ORDER = list(SOURCES)


def _landing_path(cfg, source: str) -> str:
    if cfg.is_local:
        return str(Path(cfg.landing_root) / source)
    if cfg.landing_root.startswith("/Volumes"):
        return f"{cfg.landing_root}/{source}"
    return f"/dbfs/mnt/landing/{source}"


def _ingest_local(spark, cfg, land: str, tgt: str, mode: str, label: str) -> None:
    """Local: read landing JSON in batch. Streams append (Silver dedupes), dims overwrite."""
    p = Path(land)
    if not p.exists() or not any(p.iterdir()):
        log.warning(
            "no landing data at %s — run `uv run python main.py generate` first, skipping %s",
            land,
            label,
        )
        return
    # chunk large landing dirs: read then repartition to avoid single-partition skew
    # and avoid df.count() (triggers second scan) — use write count via _success check
    df = spark.read.json(str(p))
    # parity: Auto Loader adds _metadata on CE — drop it if present so local
    # and CE bronze schemas are byte-identical (raw fields + _ingested_at only)
    if "_metadata" in df.columns:
        df = df.drop("_metadata")
    df = df.withColumn("_ingested_at", F.current_timestamp())
    # target ~128MB files: coalesce small dims, repartition high-volume streams
    try:
        parts = int(spark.conf.get("spark.sql.shuffle.partitions", "2"))
    except Exception:
        parts = 2
    parts = max(1, min(parts, 8))
    if mode == "stream" and label in ("watch_events", "cdn_logs"):
        df = df.repartition(parts)
    else:
        df = df.coalesce(min(parts, 2))
    write_mode = "overwrite" if mode == "batch" else "append"
    try:
        df.write.format("delta").mode(write_mode).option("mergeSchema", "true").saveAsTable(
            tgt
        )
    except Exception as e:
        # Re-run idempotency: if metastore lost the table but location exists, append via path
        if "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION" in str(e) and write_mode == "append":
            loc = Path(cfg.warehouse_dir) / f"{tgt.split('.')[0]}.db" / tgt.split(".")[1] if cfg.warehouse_dir else Path(".spark/warehouse") / f"{tgt.split('.')[0]}.db" / tgt.split(".")[1]
            df.write.format("delta").mode("append").option("mergeSchema", "true").save(str(loc))
            try:
                spark.sql(f"CREATE TABLE IF NOT EXISTS {tgt} USING DELTA LOCATION '{loc.as_posix()}'")
            except Exception:
                pass
            log.info("bronze %s: %s -> %s (recovered append)", label, land, tgt)
        else:
            raise
    # count via Spark UI metric not extra action — log partitions instead of count()
    log.info("bronze %s: %s -> %s", label, land, tgt)


def _ingest_databricks(
    spark, cfg, land: str, chk: str, tgt: str, mode: str, label: str
) -> None:
    """Databricks: Auto Loader (availableNow) for streams, batch overwrite for dims."""
    ensure_schema(spark, cfg.bronze_schema)
    if mode == "batch":
        df = spark.read.option("mergeSchema", "true").json(land)
        if "_metadata" in df.columns:
            df = df.drop("_metadata")
        df = df.withColumn("_ingested_at", F.current_timestamp())
        df.write.format("delta").mode("overwrite").option(
            "mergeSchema", "true"
        ).saveAsTable(tgt)
        log.info("bronze %s done -> %s", label, tgt)
        return
    (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", chk + "/schema")
        .option("cloudFiles.maxFilesPerTrigger", "500")
        .option("cloudFiles.maxBytesPerTrigger", "134217728")
        .option("cloudFiles.useIncrementalListing", "true")
        .load(land)
        .drop("_metadata")  # parity: drop Auto Loader metadata so local/CE schemas match
        .withColumn("_ingested_at", F.current_timestamp())
        .writeStream.format("delta")
        .option("checkpointLocation", chk)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(tgt)
    ).awaitTermination()
    log.info("bronze %s done -> %s", label, tgt)


def _ingest(what: str, spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    source, table, mode = SOURCES[what]
    land = _landing_path(cfg, source)
    tgt = table_fqn(cfg.bronze_schema, table)
    chk = f"{cfg.checkpoint_root}/bronze_{table}"
    ensure_schema(spark, cfg.bronze_schema)
    if cfg.is_local:
        _ingest_local(spark, cfg, land, tgt, mode, what)
    else:
        _ingest_databricks(spark, cfg, land, chk, tgt, mode, what)


def ingest_watch_events(spark=None):
    _ingest("watch_events", spark)


def ingest_subscriptions_cdc(spark=None):
    _ingest("subscriptions", spark)


def ingest_content_catalog(spark=None):
    _ingest("content", spark)


def ingest_billing(spark=None):
    _ingest("billing", spark)


def ingest_devices_cdc(spark=None):
    _ingest("devices", spark)


def ingest_profiles(spark=None):
    _ingest("profiles", spark)


def ingest_promotions(spark=None):
    _ingest("promotions", spark)


def ingest_promotion_redemptions(spark=None):
    _ingest("redemptions", spark)


def ingest_support_tickets(spark=None):
    _ingest("tickets", spark)


def ingest_cdn_stream_logs(spark=None):
    _ingest("cdn_logs", spark)


def ingest_content_ratings(spark=None):
    _ingest("ratings", spark)


def run(spark=None, what: str = "all"):
    """Entry-point for Databricks Job task."""
    m = {
        "watch_events": ingest_watch_events,
        "subscriptions": ingest_subscriptions_cdc,
        "content": ingest_content_catalog,
        "billing": ingest_billing,
        "devices": ingest_devices_cdc,
        "profiles": ingest_profiles,
        "promotions": ingest_promotions,
        "redemptions": ingest_promotion_redemptions,
        "tickets": ingest_support_tickets,
        "cdn_logs": ingest_cdn_stream_logs,
        "ratings": ingest_content_ratings,
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
    p.add_argument("--what", default="all", choices=["all", *ORDER])
    a = p.parse_args()
    run(what=a.what)
