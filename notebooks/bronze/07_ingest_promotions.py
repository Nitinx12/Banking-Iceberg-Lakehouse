# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest promotions (batch overwrite)
# MAGIC - Source: landing/promotions/*.json (generator/generate_promotions.py)
# MAGIC - Reference dimension with validity windows (starts_at/ends_at) + redemption caps
# MAGIC - Known messiness: end_before_start (0.5%), discount_pct out of range (0.5%)
# MAGIC - Batch overwrite (content_catalog pattern)

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/promotions"
TABLE = "bronze.promotions"

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

df = (
    spark.read.option("mergeSchema", "true")
    .json(LANDING)
    .withColumn("_ingested_at", F.current_timestamp())
)

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(TABLE)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT count(*) AS cnt, sum(CASE WHEN ends_at < starts_at THEN 1 ELSE 0 END) AS bad_windows FROM bronze.promotions;
