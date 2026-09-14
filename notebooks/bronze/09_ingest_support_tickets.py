# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest support_tickets via Auto Loader
# MAGIC - Source: landing/support_tickets/*.json (generator/generate_support_tickets.py)
# MAGIC - Semi-structured: nested JSON `payload` string parsed downstream in Silver (from_json)
# MAGIC - Known messiness: malformed payload JSON (1%), csat out of range (0.5%), duplicate ticket_id (0.2%)

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/support_tickets"
CHECKPOINT = "/dbfs/mnt/checkpoints/bronze_support_tickets"
TABLE = "bronze.support_tickets"

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
# MAGIC SELECT status, channel, count(*) AS cnt FROM bronze.support_tickets GROUP BY 1, 2 ORDER BY 3 DESC;
