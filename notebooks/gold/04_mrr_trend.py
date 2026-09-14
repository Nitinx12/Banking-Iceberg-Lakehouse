# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: MRR trend from billing

# COMMAND ----------
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

spark.sql("""
CREATE OR REPLACE TABLE gold.mrr_trend AS
SELECT date_trunc('month', transaction_timestamp) AS month, sum(amount) AS mrr, count(*) AS txns
FROM silver.billing
WHERE transaction_type = 'charge'
GROUP BY 1 ORDER BY 1
""")
