# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest cdn_stream_logs via Auto Loader
# MAGIC - Source: landing/cdn_stream_logs/*.json (generator/generate_cdn_stream_logs.py)
# MAGIC - High-volume QoE telemetry; Silver sessionizes on 30-min inactivity gaps
# MAGIC - Known messiness: duplicate log_id (0.5%), null bitrate (2%), late arrivals (3%),
# MAGIC   out-of-order timestamps (5%), future timestamps (0.1%)

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/cdn_stream_logs"
CHECKPOINT = "/dbfs/mnt/checkpoints/bronze_cdn_stream_logs"
TABLE = "bronze.cdn_stream_logs"

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
# MAGIC SELECT cdn_edge, count(*) AS logs, round(avg(bitrate_kbps), 0) AS avg_bitrate FROM bronze.cdn_stream_logs GROUP BY 1;
