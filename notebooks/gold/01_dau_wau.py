# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: DAU / WAU — denormalized business aggregates

# COMMAND ----------
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# Broadcast small dim if joining content_catalog
spark.sql("""
CREATE OR REPLACE TABLE gold.daily_active_users AS
SELECT to_date(event_timestamp) AS event_date, count(DISTINCT user_id) AS dau, count(*) AS events
FROM silver.watch_events
GROUP BY 1
""")

spark.sql("""
CREATE OR REPLACE TABLE gold.weekly_active_users AS
SELECT date_trunc('week', event_timestamp) AS week_start, count(DISTINCT user_id) AS wau
FROM silver.watch_events
GROUP BY 1
""")
