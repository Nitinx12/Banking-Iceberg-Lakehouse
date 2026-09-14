# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest devices_cdc via Auto Loader
# MAGIC - Source: landing/devices_cdc/*.json (generator/generate_devices_cdc.py)
# MAGIC - Second SCD2 dimension: device fleet changes (OS/app bumps, primary flips, removals)
# MAGIC - Known messiness: null os_version (2%), out-of-order change_timestamp (5%)
# MAGIC - CE workaround: trigger(availableNow=True); writes to bronze.devices_cdc

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/devices_cdc"
CHECKPOINT = "/dbfs/mnt/checkpoints/bronze_devices_cdc"
TABLE = "bronze.devices_cdc"

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
# MAGIC SELECT event_type, count(*) AS cnt FROM bronze.devices_cdc GROUP BY 1;
