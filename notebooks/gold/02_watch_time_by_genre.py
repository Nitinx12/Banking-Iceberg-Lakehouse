# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: watch time by genre — broadcast join for content_catalog (small dim)

# COMMAND ----------
from pyspark.sql import functions as F

spark.sql("CREATE DATABASE IF NOT EXISTS gold")

watch = spark.table("silver.watch_events")
catalog = spark.table("silver.content_catalog")  # or bronze if silver not yet built

# Broadcast hint for small dim
joined = watch.hint("broadcast").join(F.broadcast(catalog), on="content_id", how="left")

gold = joined.groupBy("genre").agg(F.sum("watch_duration_seconds").alias("total_watch_seconds"), F.count("*").alias("plays"))

gold.write.format("delta").mode("overwrite").saveAsTable("gold.watch_time_by_genre")
