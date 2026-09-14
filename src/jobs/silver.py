"""Silver jobs — cleaning, SCD2, quality gate + quarantine."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.config import get_config
from src.core.quality_checks import check_billing, check_watch_events
from src.core.scd2 import build_merge_sql
from src.core.transformations import clean_billing, clean_watch_events
from src.utils.engine import get_spark, table_fqn
from src.utils.logger import get_logger

log = get_logger("jobs.silver")


def silver_watch_events(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    bronze = spark.table(table_fqn(cfg.bronze_schema, "watch_events"))
    cleaned = clean_watch_events(bronze)
    res = check_watch_events(cleaned)
    if res.fail_count:
        res.quarantined.write.format("delta").mode("append").saveAsTable(f"`{cfg.catalog_name}`.{cfg.silver_schema}.quarantine")
        log.warning("quarantined %s watch_events", res.fail_count)
    # audit
    spark.createDataFrame([( "silver","watch_events", res.pass_count, res.fail_count)], ["layer","table_name","pass_count","fail_count"]).write.format("delta").mode("append").saveAsTable(cfg.audit_table)
    silver_tbl = f"`{cfg.catalog_name}`.{cfg.silver_schema}.watch_events"
    df = res.passed.withColumn("event_date", F.to_date("event_timestamp"))
    # idempotent merge
    df.createOrReplaceTempView("src_events")
    spark.sql(f"""
    MERGE INTO {silver_tbl} AS tgt
    USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) AS rn FROM src_events) WHERE rn=1) AS src
    ON tgt.event_id = src.event_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)


def silver_scd2_subscriptions(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    tgt = f"`{cfg.catalog_name}`.{cfg.silver_schema}.subscriptions_scd2"
    spark.sql(f"CREATE TABLE IF NOT EXISTS {tgt} (subscription_id STRING, user_id STRING, plan_tier STRING, status STRING, effective_date TIMESTAMP, end_date TIMESTAMP, is_current BOOLEAN) USING DELTA")
    cdc = spark.table(table_fqn(cfg.bronze_schema, "subscriptions_cdc"))
    cdc.withColumn("change_timestamp", F.to_timestamp("change_timestamp")).createOrReplaceTempView("cdc_deduped")
    spark.sql(build_merge_sql(tgt, "cdc_deduped"))
    log.info("SCD2 merge done into %s", tgt)


def silver_billing(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    bronze = spark.table(table_fqn(cfg.bronze_schema, "billing_transactions"))
    cleaned = clean_billing(bronze)
    res = check_billing(cleaned)
    if res.fail_count:
        res.quarantined.write.format("delta").mode("append").saveAsTable(f"`{cfg.catalog_name}`.{cfg.silver_schema}.quarantine")
    res.passed.write.format("delta").mode("append").saveAsTable(f"`{cfg.catalog_name}`.{cfg.silver_schema}.billing")


def run(spark=None, what: str = "all"):
    m = {"watch_events": silver_watch_events, "scd2": silver_scd2_subscriptions, "billing": silver_billing}
    if what == "all":
        for k in ["watch_events", "scd2", "billing"]:
            m[k](spark)
    elif what in m:
        m[what](spark)
    else:
        raise ValueError(what)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--what", default="all", choices=["all", "watch_events", "scd2", "billing"])
    run(what=p.parse_args().what)
