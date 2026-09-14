"""Silver jobs — cleaning, SCD2, quality gate + quarantine."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.config import get_config
from src.core.io_utils import log_audit, write_delta, write_quarantine
from src.core.quality_checks import (
    check_billing,
    check_cdn_stream_logs,
    check_content_ratings,
    check_devices_cdc,
    check_profiles,
    check_promotion_redemptions,
    check_promotions,
    check_support_tickets,
    check_watch_events,
)
from src.core.scd2 import build_merge_sql, build_merge_sql_generic
from src.core.sessionization import rollup_sessions, sessionize
from src.core.transformations import (
    clean_billing,
    clean_cdn_stream_logs,
    clean_content_ratings,
    clean_devices_cdc,
    clean_profiles,
    clean_promotion_redemptions,
    clean_promotions,
    clean_support_tickets,
    clean_watch_events,
    latest_rating_per_user_content,
)
from src.utils.engine import ensure_schemas, get_spark, table_fqn
from src.utils.logger import get_logger

log = get_logger("jobs.silver")


def _gate(spark, cfg, cleaned, check_fn, table_name: str, **check_kwargs):
    """Run the quality gate, route failures to quarantine, log audit counts."""
    res = check_fn(cleaned, **check_kwargs)
    if res.fail_count:
        write_quarantine(res.quarantined, table_fqn(cfg.silver_schema, "quarantine"))
        log.warning("quarantined %s rows for %s", res.fail_count, table_name)
    log_audit(
        spark, cfg.audit_table, "silver", table_name, res.pass_count, res.fail_count
    )
    return res


def _merge_or_create(
    spark, df, tbl: str, merge_sql: str, partition_by: list[str] | None = None
) -> None:
    """Idempotent write: MERGE when the table exists, bootstrap it otherwise."""
    if not spark.catalog.tableExists(tbl):
        write_delta(df, tbl, mode="overwrite", partition_by=partition_by)
        return
    spark.sql(merge_sql)


def silver_watch_events(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    bronze = spark.table(table_fqn(cfg.bronze_schema, "watch_events"))
    cleaned = clean_watch_events(bronze)
    res = _gate(spark, cfg, cleaned, check_watch_events, "watch_events")
    silver_tbl = table_fqn(cfg.silver_schema, "watch_events")
    df = res.passed.withColumn("event_date", F.to_date("event_timestamp"))
    # dedupe on event_id (latest timestamp wins), then idempotent merge
    df.createOrReplaceTempView("src_events")
    deduped = spark.sql(
        "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) AS rn FROM src_events) WHERE rn=1"
    ).drop("rn")
    deduped.createOrReplaceTempView("src_events_deduped")
    _merge_or_create(
        spark,
        deduped,
        silver_tbl,
        f"""
    MERGE INTO {silver_tbl} AS tgt
    USING src_events_deduped AS src
    ON tgt.event_id = src.event_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """,
    )


def silver_scd2_subscriptions(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    tgt = table_fqn(cfg.silver_schema, "subscriptions_scd2")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {tgt} (subscription_id STRING, user_id STRING, plan_tier STRING, status STRING, effective_date TIMESTAMP, end_date TIMESTAMP, is_current BOOLEAN) USING DELTA"
    )
    cdc = spark.table(table_fqn(cfg.bronze_schema, "subscriptions_cdc"))
    cdc.withColumn(
        "change_timestamp", F.to_timestamp("change_timestamp")
    ).createOrReplaceTempView("cdc_deduped")
    spark.sql(build_merge_sql(tgt, "cdc_deduped"))
    log.info("SCD2 merge done into %s", tgt)


def silver_billing(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    bronze = spark.table(table_fqn(cfg.bronze_schema, "billing_transactions"))
    cleaned = clean_billing(bronze)
    res = _gate(spark, cfg, cleaned, check_billing, "billing")
    res.passed.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(table_fqn(cfg.silver_schema, "billing"))


def silver_devices_scd2(spark=None):
    """SCD2 for the devices dimension — second SCD2 table, via the generic merge."""
    cfg = get_config()
    spark = spark or get_spark()
    tgt = table_fqn(cfg.silver_schema, "devices_scd2")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tgt} (
      device_id STRING, user_id STRING, device_type STRING, os_family STRING,
      os_version STRING, app_version STRING, is_primary BOOLEAN,
      effective_date TIMESTAMP, end_date TIMESTAMP, is_current BOOLEAN
    ) USING DELTA
    """)
    cdc = spark.table(table_fqn(cfg.bronze_schema, "devices_cdc"))
    cleaned = clean_devices_cdc(cdc)
    res = _gate(spark, cfg, cleaned, check_devices_cdc, "devices_cdc")
    res.passed.withColumn(
        "change_timestamp", F.to_timestamp("change_timestamp")
    ).createOrReplaceTempView("devices_cdc_deduped")
    spark.sql(
        build_merge_sql_generic(
            tgt,
            "devices_cdc_deduped",
            key_col="device_id",
            tracked_cols=[
                "user_id",
                "device_type",
                "os_family",
                "os_version",
                "app_version",
                "is_primary",
            ],
        )
    )
    log.info("SCD2 merge done into %s", tgt)


def silver_profiles(spark=None):
    """Small conformed dim — overwrite is idempotent."""
    cfg = get_config()
    spark = spark or get_spark()
    cleaned = clean_profiles(spark.table(table_fqn(cfg.bronze_schema, "profiles")))
    res = _gate(spark, cfg, cleaned, check_profiles, "profiles")
    write_delta(res.passed, table_fqn(cfg.silver_schema, "profiles"), mode="overwrite")


def silver_promotions(spark=None):
    """Small dim with validity windows — overwrite is idempotent."""
    cfg = get_config()
    spark = spark or get_spark()
    cleaned = clean_promotions(spark.table(table_fqn(cfg.bronze_schema, "promotions")))
    res = _gate(spark, cfg, cleaned, check_promotions, "promotions")
    write_delta(
        res.passed, table_fqn(cfg.silver_schema, "promotions"), mode="overwrite"
    )


def silver_promotion_redemptions(spark=None):
    """Bridge table — referential integrity vs promotions dim, MERGE on redemption_id."""
    cfg = get_config()
    spark = spark or get_spark()
    tbl = table_fqn(cfg.silver_schema, "promotion_redemptions")
    cleaned = clean_promotion_redemptions(
        spark.table(table_fqn(cfg.bronze_schema, "promotion_redemptions"))
    )
    promos = spark.table(table_fqn(cfg.bronze_schema, "promotions"))
    res = _gate(
        spark,
        cfg,
        cleaned,
        check_promotion_redemptions,
        "promotion_redemptions",
        promo_codes=promos,
    )
    df = res.passed
    df.createOrReplaceTempView("src_redemptions")
    deduped = spark.sql(
        "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY redemption_id ORDER BY redeemed_at DESC) AS rn FROM src_redemptions) WHERE rn=1"
    ).drop("rn")
    deduped.createOrReplaceTempView("src_redemptions_deduped")
    _merge_or_create(
        spark,
        deduped,
        tbl,
        f"""
    MERGE INTO {tbl} AS tgt
    USING src_redemptions_deduped AS src
    ON tgt.redemption_id = src.redemption_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """,
    )


def silver_support_tickets(spark=None):
    """Semi-structured — payload parsed in clean, unparseable quarantined, MERGE on ticket_id."""
    cfg = get_config()
    spark = spark or get_spark()
    tbl = table_fqn(cfg.silver_schema, "support_tickets")
    cleaned = clean_support_tickets(
        spark.table(table_fqn(cfg.bronze_schema, "support_tickets"))
    )
    res = _gate(spark, cfg, cleaned, check_support_tickets, "support_tickets")
    df = res.passed.withColumn("created_date", F.to_date("created_at"))
    df.createOrReplaceTempView("src_tickets")
    deduped = spark.sql(
        "SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY updated_at DESC) AS rn FROM src_tickets) WHERE rn=1"
    ).drop("rn")
    deduped.createOrReplaceTempView("src_tickets_deduped")
    _merge_or_create(
        spark,
        deduped,
        tbl,
        f"""
    MERGE INTO {tbl} AS tgt
    USING src_tickets_deduped AS src
    ON tgt.ticket_id = src.ticket_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """,
        partition_by=["created_date"],
    )


def silver_cdn_stream_logs(spark=None):
    """High-volume QoE telemetry — gate, then gap-based sessionization to per-session rollups."""
    cfg = get_config()
    spark = spark or get_spark()
    tbl = table_fqn(cfg.silver_schema, "cdn_stream_sessions")
    cleaned = clean_cdn_stream_logs(
        spark.table(table_fqn(cfg.bronze_schema, "cdn_stream_logs"))
    )
    res = _gate(spark, cfg, cleaned, check_cdn_stream_logs, "cdn_stream_logs")
    sessions = rollup_sessions(sessionize(res.passed))
    sessions.createOrReplaceTempView("src_sessions")
    _merge_or_create(
        spark,
        sessions,
        tbl,
        f"""
    MERGE INTO {tbl} AS tgt
    USING src_sessions AS src
    ON tgt.session_id = src.session_id AND tgt.session_number = src.session_number
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """,
        partition_by=["session_date"],
    )


def silver_content_ratings(spark=None):
    """Upsert semantics — MERGE on (user_id, content_id), latest rating wins."""
    cfg = get_config()
    spark = spark or get_spark()
    tbl = table_fqn(cfg.silver_schema, "content_ratings")
    cleaned = clean_content_ratings(
        spark.table(table_fqn(cfg.bronze_schema, "content_ratings"))
    )
    res = _gate(spark, cfg, cleaned, check_content_ratings, "content_ratings")
    df = latest_rating_per_user_content(res.passed)
    df.createOrReplaceTempView("src_ratings")
    _merge_or_create(
        spark,
        df,
        tbl,
        f"""
    MERGE INTO {tbl} AS tgt
    USING src_ratings AS src
    ON tgt.user_id = src.user_id AND tgt.content_id = src.content_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """,
    )


ORDER = [
    "watch_events",
    "scd2",
    "billing",
    "devices_scd2",
    "profiles",
    "promotions",
    "redemptions",
    "tickets",
    "cdn_logs",
    "ratings",
]


def run(spark=None, what: str = "all"):
    m = {
        "watch_events": silver_watch_events,
        "scd2": silver_scd2_subscriptions,
        "billing": silver_billing,
        "devices_scd2": silver_devices_scd2,
        "profiles": silver_profiles,
        "promotions": silver_promotions,
        "redemptions": silver_promotion_redemptions,
        "tickets": silver_support_tickets,
        "cdn_logs": silver_cdn_stream_logs,
        "ratings": silver_content_ratings,
    }
    spark = spark or get_spark()
    ensure_schemas(spark)
    targets = ORDER if what == "all" else [what]
    if what not in m:
        raise ValueError(what)
    for k in targets:
        try:
            m[k](spark)
        except Exception as e:
            # missing upstream (e.g. no landing data) → skip, don't kill the run
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                log.warning(
                    "skipping silver %s — bronze table missing (run generate + bronze first)",
                    k,
                )
                continue
            raise


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--what", default="all", choices=["all", *ORDER])
    run(what=p.parse_args().what)
