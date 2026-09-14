# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean promotions (reference dim with validity windows)
# MAGIC - bronze.promotions -> silver.promotions (overwrite — small dim, idempotent)
# MAGIC - Quality gate: null promo_code, discount_pct outside [0, 100], end before start -> quarantine

# COMMAND ----------
from src.core.quality_checks import check_promotions
from src.core.transformations import clean_promotions

bronze = spark.table("bronze.promotions")
cleaned = clean_promotions(bronze)
result = check_promotions(cleaned)

if result.fail_count > 0:
    result.quarantined.write.format("delta").mode("append").saveAsTable("silver.quarantine")
print(f"promotions gate: pass={result.pass_count} fail={result.fail_count}")

result.passed.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver.promotions")

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT discount_pct, count(*) AS promos FROM silver.promotions GROUP BY 1 ORDER BY 1;
