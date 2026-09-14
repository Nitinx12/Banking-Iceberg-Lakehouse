# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean watch_events — dedupe, normalize, partition by date
# MAGIC Idempotent MERGE on event_id.

# COMMAND ----------
from src.transformations import clean_watch_events
from src.quality_checks import check_watch_events
from src.io_utils import ensure_db, write_delta, write_quarantine, log_audit

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

bronze = spark.table("bronze.watch_events")
cleaned = clean_watch_events(bronze)

# quality gate -> quarantine
result = check_watch_events(cleaned)
if result.fail_count > 0:
    write_quarantine(result.quarantined, "silver.watch_events_quarantine")
log_audit(spark, "silver.audit_log", "silver", "watch_events", result.pass_count, result.fail_count)

# idempotent merge into silver.watch_events (partitioned by date)
silver_df = result.passed.withColumn("event_date", __import__("pyspark.sql.functions", fromlist=["to_date"]).to_date("event_timestamp"))

# Delta MERGE for idempotency
silver_df.createOrReplaceTempView("src_events")
spark.sql("""
MERGE INTO silver.watch_events AS tgt
USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) AS rn FROM src_events) WHERE rn=1) AS src
ON tgt.event_id = src.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------
# MAGIC %sql
# MAGIC OPTIMIZE silver.watch_events ZORDER BY (user_id);
