# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze: ingest watch_events via Auto Loader
# MAGIC - Source: landing/watch_events/*.json
# MAGIC - CE workaround: trigger(availableNow=True) instead of continuous stream
# MAGIC - Writes to bronze.watch_events (Hive DB, no Unity Catalog on CE)

# COMMAND ----------
# MAGIC %pip install -r ../../requirements.txt  # or %pip install delta-spark
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
from pyspark.sql import functions as F

LANDING = "/dbfs/mnt/landing/watch_events"
CHECKPOINT = "/dbfs/mnt/checkpoints/bronze_watch_events"
TABLE = "bronze.watch_events"

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

# Merge schema evolution enabled
query = (
    df.writeStream.format("delta")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TABLE)
)

query.awaitTermination()

# COMMAND ----------
# MAGIC %md
# MAGIC **Production upgrade path:** replace `trigger(availableNow=True)` with `trigger(continuous="1 second")` or
# MAGIC `trigger(availableNow=False)` under a Databricks Workflow job cluster that stays alive; point `LANDING` to
# MAGIC S3/ADLS via Unity Catalog external location.

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT count(*) AS cnt, max(_ingested_at) AS last_ingested FROM bronze.watch_events;
