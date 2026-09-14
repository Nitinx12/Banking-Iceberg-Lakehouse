-- sql/init_schema.sql
-- StreamFlix Lakehouse — catalog `tham`, schema `init_schema` + medallion schemas
-- Works on both:
--   - Community Edition (Hive metastore) -> CREATE DATABASE
--   - Unity Catalog (paid) -> CREATE CATALOG + CREATE SCHEMA
-- Run via: Databricks SQL editor, or `databricks sql execute --sql init_schema.sql`

-- ============================================================
-- 1. Catalog `tham`
-- ============================================================
-- UC path (no-op on Hive): if your workspace has Unity Catalog enabled,
-- this creates the catalog backed by an external location (replace storage).
-- On CE this will fail — fall back to Hive databases in §2.
CREATE CATALOG IF NOT EXISTS tham
  MANAGED LOCATION 'abfss://lakehouse@<storage-account>.dfs.core.windows.net/tham'
  COMMENT 'StreamFlix lakehouse — medallion architecture (plan §3). UC migration from Hive bronze/silver/gold.';

-- ============================================================
-- 2. Hive fallback (Community Edition) — databases = catalogs
-- ============================================================
CREATE DATABASE IF NOT EXISTS tham
  COMMENT 'CE fallback: catalog tham as Hive database';

-- Medallion databases (CE) / schemas (UC external location would be S3/ADLS)
CREATE DATABASE IF NOT EXISTS bronze COMMENT 'Bronze — raw, Auto Loader, mergeSchema';
CREATE DATABASE IF NOT EXISTS silver COMMENT 'Silver — cleaned, deduped, SCD2, quarantine';
CREATE DATABASE IF NOT EXISTS gold   COMMENT 'Gold — business aggregates (DAU/WAU, churn, MRR)';

-- ============================================================
-- 3. UC schemas under catalog `tham` (no-op on Hive until UC enabled)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS tham.bronze COMMENT 'mirror of Hive bronze';
CREATE SCHEMA IF NOT EXISTS tham.silver COMMENT 'mirror of Hive silver';
CREATE SCHEMA IF NOT EXISTS tham.gold   COMMENT 'mirror of Hive gold';

-- canonical entry-point schema requested: tham.init_schema
CREATE SCHEMA IF NOT EXISTS tham.init_schema
  COMMENT 'StreamFlix init — run before ingest: audit_log + quarantine scaffolding';
-- Hive fallback for same (so both paths have tham.init_schema)
CREATE DATABASE IF NOT EXISTS tham_init_schema COMMENT 'Hive fallback for tham.init_schema';

-- ============================================================
-- 4. Core tables in tham.init_schema (audit + lineage scaffolding)
-- ============================================================
-- audit log: silver.audit_log is also used by notebooks; this is the UC-canonical copy
CREATE TABLE IF NOT EXISTS tham.init_schema.audit_log (
  layer STRING,
  table_name STRING,
  pass_count LONG,
  fail_count LONG,
  run_at TIMESTAMP
) USING DELTA
  COMMENT 'per-run pass/fail counts from the Silver quality gate (src/quality_checks.py)';

-- Hive fallback copy for CE notebooks that write to silver.audit_log
CREATE TABLE IF NOT EXISTS silver.audit_log (
  layer STRING,
  table_name STRING,
  pass_count LONG,
  fail_count LONG,
  run_at TIMESTAMP
) USING DELTA;

-- lineage tracker (docs/lineage.md has the human version)
CREATE TABLE IF NOT EXISTS tham.init_schema.lineage (
  source_table STRING,
  target_table STRING,
  transform STRING,
  updated_at TIMESTAMP
) USING DELTA;

-- ============================================================
-- 5. Grants (UC only — no-op on Hive)
-- ============================================================
-- GRANT USE CATALOG ON CATALOG tham TO `data-engineers`;
-- GRANT USE SCHEMA, SELECT, MODIFY ON SCHEMA tham.init_schema TO `data-engineers`;
-- GRANT SELECT ON SCHEMA tham.gold TO `bi-consumers`;

-- ============================================================
-- 6. Verify
-- ============================================================
SHOW CATALOGS;
SHOW SCHEMAS IN tham;
SHOW TABLES IN tham.init_schema;
