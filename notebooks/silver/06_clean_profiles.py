# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean profiles (conformed household dimension)
# MAGIC - bronze.profiles -> silver.profiles (overwrite — small dim, idempotent)
# MAGIC - Quality gate: null profile_id, null user_id, invalid language, duplicate profile_id -> quarantine

# COMMAND ----------
from src.core.quality_checks import check_profiles
from src.core.transformations import clean_profiles

bronze = spark.table("bronze.profiles")
cleaned = clean_profiles(bronze)
result = check_profiles(cleaned)

if result.fail_count > 0:
    result.quarantined.write.format("delta").mode("append").saveAsTable("silver.quarantine")
print(f"profiles gate: pass={result.pass_count} fail={result.fail_count}")

result.passed.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("silver.profiles")

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT language, is_kids, count(*) AS profiles FROM silver.profiles GROUP BY 1, 2 ORDER BY 3 DESC;
