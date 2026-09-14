# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest profiles (batch overwrite)
# MAGIC - Source: landing/profiles/*.json (generator/generate_profiles.py)
# MAGIC - Small conformed dimension: 1 user -> N household sub-profiles
# MAGIC - Known messiness: invalid language code (1%), duplicate profile_id (0.5%)
# MAGIC - Batch overwrite (content_catalog pattern) — not event data, no Auto Loader

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/profiles"
TABLE = "bronze.profiles"

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

df = (
    spark.read.option("mergeSchema", "true")
    .json(LANDING)
    .withColumn("_ingested_at", F.current_timestamp())
)

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(TABLE)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT count(*) AS cnt, count(DISTINCT user_id) AS users FROM bronze.profiles;
