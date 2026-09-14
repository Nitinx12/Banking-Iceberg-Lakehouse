# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: clean support_tickets (semi-structured payload)
# MAGIC - bronze.support_tickets -> silver.support_tickets
# MAGIC - Nested JSON `payload` parsed with from_json (explicit schema) and flattened
# MAGIC - Malformed payloads -> quarantine (unparseable_payload), not dropped
# MAGIC - Partitioned by created_date; idempotent MERGE on ticket_id

# COMMAND ----------
from pyspark.sql import functions as F

from src.core.quality_checks import check_support_tickets
from src.core.transformations import clean_support_tickets

TARGET = "silver.support_tickets"

bronze = spark.table("bronze.support_tickets")
cleaned = clean_support_tickets(bronze)  # parses payload -> category, subcategory, payload_device_type, ...
result = check_support_tickets(cleaned)

if result.fail_count > 0:
    result.quarantined.write.format("delta").mode("append").saveAsTable("silver.quarantine")
print(f"support_tickets gate: pass={result.pass_count} fail={result.fail_count} (incl. unparseable payloads)")

df = result.passed.withColumn("created_date", F.to_date("created_at"))
if not spark.catalog.tableExists(TARGET):
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("created_date").saveAsTable(TARGET)
else:
    df.createOrReplaceTempView("src_tickets")
    spark.sql(f"""
    MERGE INTO {TARGET} AS tgt
    USING (SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY updated_at DESC) AS rn FROM src_tickets) WHERE rn=1) AS src
    ON tgt.ticket_id = src.ticket_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT category, count(*) AS tickets FROM silver.support_tickets GROUP BY 1 ORDER BY 2 DESC;
