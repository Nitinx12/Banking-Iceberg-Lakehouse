# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean content_ratings (upsert semantics)
# MAGIC - bronze.content_ratings -> silver.content_ratings
# MAGIC - Natural key is (user_id, content_id), NOT rating_id: re-ratings carry a
# MAGIC   new rating_id, so dedupe is latest-wins by updated_at
# MAGIC - Quality gate runs BEFORE latest-wins so bad re-ratings are quarantined, not silently dropped
# MAGIC - Idempotent MERGE on (user_id, content_id)

# COMMAND ----------
from src.core.quality_checks import check_content_ratings
from src.core.transformations import clean_content_ratings, latest_rating_per_user_content

TARGET = "silver.content_ratings"

bronze = spark.table("bronze.content_ratings")
cleaned = clean_content_ratings(bronze)
result = check_content_ratings(cleaned)

if result.fail_count > 0:
    result.quarantined.write.format("delta").mode("append").saveAsTable("silver.quarantine")
print(f"content_ratings gate: pass={result.pass_count} fail={result.fail_count}")

df = latest_rating_per_user_content(result.passed)
if not spark.catalog.tableExists(TARGET):
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(TARGET)
else:
    df.createOrReplaceTempView("src_ratings")
    spark.sql(f"""
    MERGE INTO {TARGET} AS tgt
    USING src_ratings AS src
    ON tgt.user_id = src.user_id AND tgt.content_id = src.content_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT rating, count(*) AS cnt FROM silver.content_ratings GROUP BY 1 ORDER BY 1;
