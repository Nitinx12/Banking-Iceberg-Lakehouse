# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest promotion_redemptions via Auto Loader
# MAGIC - Source: landing/promotion_redemptions/*.json (generator/generate_promotion_redemptions.py)
# MAGIC - Factless fact / bridge table: many-to-many user <-> promo
# MAGIC - Known messiness: duplicate redemption_id (0.5%), orphaned promo_code (1%), future redeemed_at (0.1%)

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/promotion_redemptions"
CHECKPOINT = "/dbfs/mnt/checkpoints/bronze_promotion_redemptions"
TABLE = "bronze.promotion_redemptions"

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
# MAGIC SELECT count(*) AS cnt, count(DISTINCT user_id) AS users, count(DISTINCT promo_code) AS promos FROM bronze.promotion_redemptions;
