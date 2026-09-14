# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean promotion_redemptions (bridge table)
# MAGIC - bronze.promotion_redemptions -> silver.promotion_redemptions
# MAGIC - Referential integrity: orphaned promo_code (not in bronze.promotions) -> quarantine
# MAGIC - Idempotent: MERGE on redemption_id

# COMMAND ----------
from src.core.quality_checks import check_promotion_redemptions
from src.core.transformations import clean_promotion_redemptions

TARGET = "silver.promotion_redemptions"

bronze = spark.table("bronze.promotion_redemptions")
promos = spark.table("bronze.promotions")
cleaned = clean_promotion_redemptions(bronze)
result = check_promotion_redemptions(cleaned, promo_codes=promos)

if result.fail_count > 0:
    result.quarantined.write.format("delta").mode("append").saveAsTable("silver.quarantine")
print(f"promotion_redemptions gate: pass={result.pass_count} fail={result.fail_count} (incl. orphaned promo codes)")

df = result.passed
if not spark.catalog.tableExists(TARGET):
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(TARGET)
else:
    df.createOrReplaceTempView("src_redemptions")
    spark.sql(f"""
    MERGE INTO {TARGET} AS tgt
    USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY redemption_id ORDER BY redeemed_at DESC) AS rn FROM src_redemptions) WHERE rn=1) AS src
    ON tgt.redemption_id = src.redemption_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT count(*) AS redemptions, count(DISTINCT user_id) AS users FROM silver.promotion_redemptions;
