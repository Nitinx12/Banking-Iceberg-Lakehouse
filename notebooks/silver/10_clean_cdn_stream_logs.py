# Databricks notebook source
# MAGIC %md
# MAGIC # Silver: cdn_stream_logs -> cdn_stream_sessions (sessionization)
# MAGIC - Quality gate: nulls, duplicates, negative rebuffer, future/late timestamps -> quarantine
# MAGIC - Gap-based sessionization (src/core/sessionization.py): new session when
# MAGIC   inactivity gap > 30 minutes within a session_id
# MAGIC - Streaming upgrade path: replace the batch window with Structured Streaming
# MAGIC   `session_window(event_timestamp, "30 minutes")` under a continuous trigger
# MAGIC - Partitioned by session_date; idempotent MERGE on (session_id, session_number)

# COMMAND ----------
from src.core.quality_checks import check_cdn_stream_logs
from src.core.sessionization import rollup_sessions, sessionize
from src.core.transformations import clean_cdn_stream_logs

TARGET = "silver.cdn_stream_sessions"

bronze = spark.table("bronze.cdn_stream_logs")
cleaned = clean_cdn_stream_logs(bronze)
result = check_cdn_stream_logs(cleaned)

if result.fail_count > 0:
    result.quarantined.write.format("delta").mode("append").saveAsTable("silver.quarantine")
print(f"cdn_stream_logs gate: pass={result.pass_count} fail={result.fail_count}")

sessions = rollup_sessions(sessionize(result.passed))
if not spark.catalog.tableExists(TARGET):
    sessions.write.format("delta").mode("overwrite").option("mergeSchema", "true").partitionBy("session_date").saveAsTable(TARGET)
else:
    sessions.createOrReplaceTempView("src_sessions")
    spark.sql(f"""
    MERGE INTO {TARGET} AS tgt
    USING src_sessions AS src
    ON tgt.session_id = src.session_id AND tgt.session_number = src.session_number
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------
# MAGIC %sql
# MAGIC SELECT device_type, count(*) AS sessions, round(avg(total_rebuffer_ms), 0) AS avg_rebuffer_ms
# MAGIC FROM silver.cdn_stream_sessions GROUP BY 1 ORDER BY 2 DESC;
