# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest billing_transactions

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/billing"
CHECKPOINT = "/dbfs/mnt/checkpoints/bronze_billing"
TABLE = "bronze.billing_transactions"

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", CHECKPOINT + "/schema")
    .load(LANDING)
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    df.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TABLE)
).awaitTermination()
