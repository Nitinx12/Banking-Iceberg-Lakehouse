# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Setup catalog / Hive DBs (CE workaround for Unity Catalog)
# MAGIC Creates `bronze`, `silver`, `gold` Hive databases and audit/quarantine scaffolding.
# MAGIC Production upgrade path (documented for interview): Unity Catalog catalogs
# MAGIC with external locations + grants (`CREATE CATALOG streamflix ...`).

# COMMAND ----------
# MAGIC %md
# MAGIC ### Secrets handling
# MAGIC Locally secrets come from `.env` (see `src/config.py`). On this cluster they
# MAGIC come from Databricks Secret Scopes / widgets — `.env` is the documented stand-in
# MAGIC for Secrets + KMS (README § Known limitations).

# COMMAND ----------
from src.config import get_config

cfg = get_config()
print(f"env={cfg.env} bronze={cfg.bronze_db} silver={cfg.silver_db} gold={cfg.gold_db}")
print(f"landing={cfg.landing_root} checkpoints={cfg.checkpoint_root}")

# COMMAND ----------
# dbutils.widgets.text("env", "dev")  # enable if you want widget override

for db in [cfg.bronze_db, cfg.silver_db, cfg.gold_db]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    print(f"created/verified {db}")

# audit log (silver) — partitioned by run_at date in practice
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {cfg.silver_db}.audit_log (
  layer STRING, table_name STRING, pass_count LONG, fail_count LONG, run_at TIMESTAMP
) USING DELTA
""")

# quarantine tables are created on first write with partitioning by _q_date (see src/io_utils.py)

# COMMAND ----------
# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------
# MAGIC %md
# MAGIC **Production path:** `bronze`/`silver`/`gold` Hive DBs → `streamflix.bronze` etc. UC catalogs:
# MAGIC ```sql
# MAGIC CREATE CATALOG IF NOT EXISTS streamflix;
# MAGIC CREATE SCHEMA IF NOT EXISTS streamflix.bronze MANAGED LOCATION 's3://.../bronze';
# MAGIC GRANT SELECT ON CATALOG streamflix TO `data-consumers`;
# MAGIC ```
