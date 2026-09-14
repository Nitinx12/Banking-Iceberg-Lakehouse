# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest content_ratings via Auto Loader
# MAGIC - Source: landing/content_ratings/*.json (generator/generate_content_ratings.py)
# MAGIC - User reviews with re-ratings: Silver dedupes on (user_id, content_id) latest-wins,
# MAGIC   NOT on rating_id PK (upsert semantics)
# MAGIC - Known messiness: rating out of range (0.5%), null user_id (0.5%), re-ratings (5%)

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/content_ratings"
CHECKPOINT = "/dbfs/mnt/checkpoints/bronze_content_ratings"
TABLE = "bronze.content_ratings"

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load(LANDING)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_batch_id", F.expr("uuid()"))
)

query = (
    df.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TABLE)
)

query.awaitTermination()

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT rating, count(*) AS cnt FROM bronze.content_ratings GROUP BY 1 ORDER BY 1;
