# Databricks notebook source
# MAGIC %md
# MAGIC # Gold: content engagement by genre
# MAGIC - silver.content_ratings x content_catalog (broadcast join)
# MAGIC - Answers: "which genres do users rate highest, and where is engagement deepest?"
# MAGIC - Uses the latest-wins ratings (one row per user x content)

# COMMAND ----------
from pyspark.sql import functions as F

ratings = spark.table("silver.content_ratings")
catalog = spark.table("bronze.content_catalog")

joined = ratings.join(F.broadcast(catalog), on="content_id", how="left")

(
    joined.groupBy("genre")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.lit(1)).alias("n_ratings"),
        F.countDistinct("content_id").alias("n_content"),
    )
    .write.format("delta")
    .mode("overwrite")
    .saveAsTable("gold.content_engagement")
)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT genre, avg_rating, n_ratings, n_content FROM gold.content_engagement ORDER BY avg_rating DESC;
