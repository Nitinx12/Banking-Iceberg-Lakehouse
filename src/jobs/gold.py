"""Gold jobs — business aggregates, broadcast joins for small dims."""

from __future__ import annotations

from pyspark.sql import functions as F

from src.config import get_config
from src.utils.engine import ensure_schemas, get_spark, table_fqn
from src.utils.logger import get_logger

log = get_logger("jobs.gold")


def _overwrite_from_sql(spark, sql: str, tbl: str) -> None:
    """Materialize a SQL query as a Delta table, replacing what's there.

    Uses the DataFrame overwrite path rather than CREATE OR REPLACE TABLE AS
    SELECT — RTAS isn't supported by the local (non-UC) session catalog.
    """
    spark.sql(sql).write.format("delta").mode("overwrite").saveAsTable(tbl)


def gold_dau_wau(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    watch = table_fqn(cfg.silver_schema, "watch_events")
    # use partition column event_date for pruning (silver.watch_events is
    # partitioned by event_date); avoids re-deriving from event_timestamp
    _overwrite_from_sql(
        spark,
        f"SELECT event_date, count(DISTINCT user_id) AS dau FROM {watch} WHERE event_date IS NOT NULL GROUP BY 1",
        table_fqn(cfg.gold_schema, "daily_active_users"),
    )
    _overwrite_from_sql(
        spark,
        f"SELECT date_trunc('week', event_date) AS week_start, count(DISTINCT user_id) AS wau FROM {watch} WHERE event_date IS NOT NULL GROUP BY 1",
        table_fqn(cfg.gold_schema, "weekly_active_users"),
    )


def _catalog_genres(spark, cfg):
    """Content catalog projected to the join/genre columns — the dim's own `rating`
    (PG-13-style) must not collide with content_ratings.rating in downstream joins."""
    return spark.table(table_fqn(cfg.bronze_schema, "content_catalog")).select(
        "content_id", "genre"
    )


def gold_watch_time_by_genre(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    watch = spark.table(table_fqn(cfg.silver_schema, "watch_events"))
    joined = watch.hint("broadcast").join(
        F.broadcast(_catalog_genres(spark, cfg)), on="content_id", how="left"
    )
    joined.groupBy("genre").agg(
        F.sum("watch_duration_seconds").alias("total_watch_seconds")
    ).write.format("delta").mode("overwrite").saveAsTable(
        table_fqn(cfg.gold_schema, "watch_time_by_genre")
    )


def gold_churn_mrr(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    _overwrite_from_sql(
        spark,
        f"SELECT * FROM {table_fqn(cfg.silver_schema, 'subscriptions_scd2')} WHERE status='canceled'",
        table_fqn(cfg.gold_schema, "churn_signals"),
    )
    _overwrite_from_sql(
        spark,
        f"SELECT date_trunc('month', transaction_timestamp) AS month, sum(amount) AS mrr FROM {table_fqn(cfg.silver_schema, 'billing')} WHERE transaction_type='charge' GROUP BY 1",
        table_fqn(cfg.gold_schema, "mrr_trend"),
    )


def gold_qoe_by_device(spark=None):
    """QoE per device type and genre: sessions x devices SCD2 (current) x catalog (broadcast)."""
    cfg = get_config()
    spark = spark or get_spark()
    sessions = spark.table(table_fqn(cfg.silver_schema, "cdn_stream_sessions"))
    # devices_scd2 current is tiny (500 rows) — broadcast to avoid shuffle
    devices = F.broadcast(
        spark.table(table_fqn(cfg.silver_schema, "devices_scd2")).filter(
            F.col("is_current")
        )
    )
    joined = sessions.join(devices, on=["user_id", "device_type"], how="inner").join(
        F.broadcast(_catalog_genres(spark, cfg)), on="content_id", how="left"
    )
    joined.groupBy("session_date", "device_type", "genre").agg(
        F.count(F.lit(1)).alias("n_sessions"),
        F.avg("avg_bitrate_kbps").alias("avg_bitrate_kbps"),
        F.avg("startup_ms").alias("avg_startup_ms"),
        F.sum("total_rebuffer_ms").alias("total_rebuffer_ms"),
    ).write.format("delta").mode("overwrite").saveAsTable(
        table_fqn(cfg.gold_schema, "qoe_by_device")
    )


def gold_promo_effectiveness(spark=None):
    """Bridge-table attribution: redemptions x promotions x billing revenue per promo_code."""
    cfg = get_config()
    spark = spark or get_spark()
    _overwrite_from_sql(
        spark,
        f"""
    SELECT r.promo_code,
           p.discount_pct,
           count(*) AS redemptions,
           count(DISTINCT r.user_id) AS unique_users,
           round(sum(b.amount), 2) AS attributed_revenue
    FROM {table_fqn(cfg.silver_schema, "promotion_redemptions")} r
    LEFT JOIN {table_fqn(cfg.silver_schema, "promotions")} p
      ON r.promo_code = p.promo_code
    LEFT JOIN {table_fqn(cfg.silver_schema, "billing")} b
      ON r.user_id = b.user_id
     AND to_date(date_trunc('month', b.transaction_timestamp)) = to_date(r.billing_period_start)
    GROUP BY r.promo_code, p.discount_pct
    """,
        table_fqn(cfg.gold_schema, "promo_effectiveness"),
    )


def gold_support_ticket_summary(spark=None):
    cfg = get_config()
    spark = spark or get_spark()
    _overwrite_from_sql(
        spark,
        f"""
    SELECT date_trunc('week', created_at) AS week_start,
           channel,
           count(*) AS tickets,
           round(avg(csat_score), 2) AS avg_csat
    FROM {table_fqn(cfg.silver_schema, "support_tickets")}
    GROUP BY 1, 2
    """,
        table_fqn(cfg.gold_schema, "support_ticket_summary"),
    )


def gold_content_engagement(spark=None):
    """Ratings engagement by genre — broadcast join against the content catalog."""
    cfg = get_config()
    spark = spark or get_spark()
    ratings = spark.table(table_fqn(cfg.silver_schema, "content_ratings"))
    joined = ratings.join(
        F.broadcast(_catalog_genres(spark, cfg)), on="content_id", how="left"
    )
    joined.groupBy("genre").agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.lit(1)).alias("n_ratings"),
        F.countDistinct("content_id").alias("n_content"),
    ).write.format("delta").mode("overwrite").saveAsTable(
        table_fqn(cfg.gold_schema, "content_engagement")
    )


ORDER = {
    "dau_wau": gold_dau_wau,
    "genre": gold_watch_time_by_genre,
    "churn_mrr": gold_churn_mrr,
    "qoe": gold_qoe_by_device,
    "promo": gold_promo_effectiveness,
    "support": gold_support_ticket_summary,
    "engagement": gold_content_engagement,
}


def run(spark=None, what: str = "all"):
    spark = spark or get_spark()
    ensure_schemas(spark)
    if what != "all" and what not in ORDER:
        raise ValueError(what)
    targets = list(ORDER.items()) if what == "all" else [(what, ORDER[what])]
    for name, fn in targets:
        try:
            fn(spark)
        except Exception as e:
            # missing upstream (e.g. no landing data) → skip, don't kill the run
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                log.warning(
                    "skipping gold %s — upstream table missing (run generate + bronze + silver first)",
                    name,
                )
                continue
            raise


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--what", default="all", choices=["all", *ORDER])
    run(what=p.parse_args().what)
