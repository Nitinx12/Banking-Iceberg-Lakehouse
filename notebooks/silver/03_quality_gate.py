# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: centralized quality gate + quarantine + audit
# MAGIC Routes failures to _quarantine, logs pass/fail counts.

# COMMAND ----------
from src.quality_checks import check_watch_events, check_billing
from src.io_utils import write_quarantine, log_audit

# watch_events gate is in 01_clean_watch_events; this notebook demonstrates billing gate
billing = spark.table("bronze.billing_transactions")
from src.transformations import clean_billing

cleaned = clean_billing(billing)
result = check_billing(cleaned)
if result.fail_count > 0:
    write_quarantine(result.quarantined, "silver.billing_quarantine")
log_audit(spark, "silver.audit_log", "silver", "billing", result.pass_count, result.fail_count)
result.passed.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("silver.billing")
