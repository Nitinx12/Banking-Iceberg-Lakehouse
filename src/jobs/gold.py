"""Gold jobs — business aggregates, broadcast joins for small dims."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.config import get_config
from src.utils.engine import get_spark, table_fqn
from src.utils.logger import get_logger

log = get_logger("jobs.gold")


def gold_dau_wau(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg.catalog_name}`.{cfg.gold_schema}")
    spark.sql(f"""
    CREATE OR REPLACE TABLE `{cfg.catalog_name}`.{cfg.gold_schema}.daily_active_users AS
    SELECT to_date(event_timestamp) AS event_date, count(DISTINCT user_id) AS dau FROM `{cfg.catalog_name}`.{cfg.silver_schema}.watch_events GROUP BY 1
    """)
    spark.sql(f"""
    CREATE OR REPLACE TABLE `{cfg.catalog_name}`.{cfg.gold_schema}.weekly_active_users AS
    SELECT date_trunc('week', event_timestamp) AS week_start, count(DISTINCT user_id) AS wau FROM `{cfg.catalog_name}`.{cfg.silver_schema}.watch_events GROUP BY 1
    """)


def gold_watch_time_by_genre(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    watch = spark.table(table_fqn(cfg.silver_schema, "watch_events"))
    catalog = spark.table(table_fqn(cfg.bronze_schema, "content_catalog"))
    joined = watch.hint("broadcast").join(F.broadcast(catalog), on="content_id", how="left")
    joined.groupBy("genre").agg(F.sum("watch_duration_seconds").alias("total_watch_seconds")).write.format("delta").mode("overwrite").saveAsTable(f"`{cfg.catalog_name}`.{cfg.gold_schema}.watch_time_by_genre")


def gold_churn_mrr(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    spark.sql(f"CREATE OR REPLACE TABLE `{cfg.catalog_name}`.{cfg.gold_schema}.churn_signals AS SELECT * FROM `{cfg.catalog_name}`.{cfg.silver_schema}.subscriptions_scd2 WHERE status='canceled'")
    spark.sql(f"CREATE OR REPLACE TABLE `{cfg.catalog_name}`.{cfg.gold_schema}.mrr_trend AS SELECT date_trunc('month', transaction_timestamp) AS month, sum(amount) AS mrr FROM `{cfg.catalog_name}`.{cfg.silver_schema}.billing WHERE transaction_type='charge' GROUP BY 1")


def run(spark=None, what: str = "all"):
    if what in ("all", "dau_wau"):
        gold_dau_wau(spark)
    if what in ("all", "genre"):
        gold_watch_time_by_genre(spark)
    if what in ("all", "churn_mrr"):
        gold_churn_mrr(spark)


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--what", default="all")
    run(what=p.parse_args().what)
