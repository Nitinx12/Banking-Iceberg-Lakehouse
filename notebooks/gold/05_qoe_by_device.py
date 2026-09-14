# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: QoE by device type and genre
# MAGIC - silver.cdn_stream_sessions x silver.devices_scd2 (is_current) x content_catalog (broadcast)
# MAGIC - Answers: "which device types / genres have the worst streaming quality?"
# MAGIC - Rebuffer ratio and startup latency per (session_date, device_type, genre)

# COMMAND ----------
from pyspark.sql import functions as F

sessions = spark.table("silver.cdn_stream_sessions")
devices = spark.table("silver.devices_scd2").filter(F.col("is_current"))
catalog = spark.table("bronze.content_catalog")

joined = (
    sessions.join(devices, on=["user_id", "device_type"], how="inner")
    .join(F.broadcast(catalog), on="content_id", how="left")
)

(
    joined.groupBy("session_date", "device_type", "genre")
    .agg(
        F.count(F.lit(1)).alias("n_sessions"),
        F.avg("avg_bitrate_kbps").alias("avg_bitrate_kbps"),
        F.avg("startup_ms").alias("avg_startup_ms"),
        F.sum("total_rebuffer_ms").alias("total_rebuffer_ms"),
    )
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("gold.qoe_by_device")
)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT device_type, genre, n_sessions, avg_bitrate_kbps, avg_startup_ms
# MAGIC FROM gold.qoe_by_device ORDER BY session_date DESC, device_type LIMIT 20;
