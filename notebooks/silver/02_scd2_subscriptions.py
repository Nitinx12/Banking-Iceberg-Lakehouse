# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: SCD2 for subscriptions
# MAGIC Uses src/scd2.py MERGE logic; idempotent re-run proven by tests.

# COMMAND ----------
from src.core.scd2 import build_merge_sql

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("""
CREATE TABLE IF NOT EXISTS silver.subscriptions_scd2 (
  subscription_id STRING, user_id STRING, plan_tier STRING, status STRING,
  effective_date TIMESTAMP, end_date TIMESTAMP, is_current BOOLEAN
) USING DELTA PARTITIONED BY (is_current)
""")

cdc = spark.table("bronze.subscriptions_cdc")
# Ensure typed + deduped view
cdc_typed = cdc.withColumn("change_timestamp", __import__("pyspark.sql.functions", fromlist=["to_timestamp"]).to_timestamp("change_timestamp"))
cdc_typed.createOrReplaceTempView("cdc_deduped")

spark.sql(build_merge_sql("silver.subscriptions_scd2", "cdc_deduped"))
