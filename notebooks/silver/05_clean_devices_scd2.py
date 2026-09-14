# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: devices SCD2 (second slowly-changing dimension)
# MAGIC - bronze.devices_cdc -> silver.devices_scd2 via generic MERGE (src/core/scd2.py)
# MAGIC - Quality gate: null device_id, invalid event_type, future timestamp -> quarantine
# MAGIC - Idempotent: re-running the same CDC batch does not duplicate history rows

# COMMAND ----------
from pyspark.sql import functions as F

from src.core.quality_checks import check_devices_cdc
from src.core.scd2 import build_merge_sql_generic
from src.core.transformations import clean_devices_cdc

TARGET = "silver.devices_scd2"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {TARGET} (
  device_id STRING, user_id STRING, device_type STRING, os_family STRING,
  os_version STRING, app_version STRING, is_primary BOOLEAN,
  effective_date TIMESTAMP, end_date TIMESTAMP, is_current BOOLEAN
) USING DELTA
""")

cdc = spark.table("bronze.devices_cdc")
cleaned = clean_devices_cdc(cdc)
result = check_devices_cdc(cleaned)

if result.fail_count > 0:
    result.quarantined.write.format("delta").mode("append").saveAsTable("silver.quarantine")
print(f"devices_cdc gate: pass={result.pass_count} fail={result.fail_count}")

result.passed.withColumn("change_timestamp", F.to_timestamp("change_timestamp")).createOrReplaceTempView("devices_cdc_deduped")

spark.sql(
    build_merge_sql_generic(
        TARGET,
        "devices_cdc_deduped",
        key_col="device_id",
        tracked_cols=["user_id", "device_type", "os_family", "os_version", "app_version", "is_primary"],
    )
)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT device_type, count(*) AS history_rows, sum(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_rows
# MAGIC FROM silver.devices_scd2 GROUP BY 1;
