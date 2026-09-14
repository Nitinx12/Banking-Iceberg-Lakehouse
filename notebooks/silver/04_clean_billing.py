# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean billing — dedupe on transaction_id

# COMMAND ----------
from src.transformations import clean_billing

spark.sql("CREATE DATABASE IF NOT EXISTS silver")
bronze = spark.table("bronze.billing_transactions")
silver = clean_billing(bronze)
silver.withColumn("txn_date", __import__("pyspark.sql.functions", fromlist=["to_date"]).to_date("transaction_timestamp")) \
      .write.format("delta").mode("append").partitionBy("txn_date").saveAsTable("silver.billing")
