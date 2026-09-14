# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: churn signals — cancellations, downgrades, engagement drop

# COMMAND ----------
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

spark.sql("""
CREATE OR REPLACE TABLE gold.churn_signals AS
SELECT user_id, plan_tier, status, effective_date, end_date
FROM silver.subscriptions_scd2
WHERE status = 'canceled' OR plan_tier = 'basic'  -- downgrade proxy
""")
