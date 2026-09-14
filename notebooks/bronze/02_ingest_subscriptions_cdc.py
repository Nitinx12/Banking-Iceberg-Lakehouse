# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest subscriptions CDC change events

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/subscriptions_cdc"
CHECKPOINT = "/dbfs/mnt/checkpoints/bronze_subscriptions_cdc"
TABLE = "bronze.subscriptions_cdc"

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
)

(spark.readStream.format("cloudFiles") if False else df).writeStream  # keep linter happy

(
    df.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TABLE)
).awaitTermination()
