# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest content_catalog (batch — slow-changing dimension)

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/content_catalog"
TABLE = "bronze.content_catalog"

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

df = spark.read.option("mergeSchema", "true").json(LANDING).withColumn("_ingested_at", F.current_timestamp())

# Idempotent overwrite by file batch — Delta mergeSchema preserves evolution
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(TABLE)
