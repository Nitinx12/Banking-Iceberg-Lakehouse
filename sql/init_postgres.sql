-- sql/init_postgres.sql — banking_dw + ops + serving/rt scaffold (Architecture 8)
-- Runs once on first `docker compose --profile core up postgres`
-- Idempotent with IF NOT EXISTS where possible

-- Databases: postgres init creates POSTGRES_WAREHOUSE_DB (banking_dw) automatically.
-- We create the other two here.
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SELECT 'CREATE DATABASE iceberg_catalog' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'iceberg_catalog')\gexec

-- Connect to banking_dw for remaining objects (compose POSTGRES_DB)
\c banking_dw

-- Schemas
CREATE SCHEMA IF NOT EXISTS serving;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS rt;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- Roles (passwords injected via env in compose; defaults here for local)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etl_writer') THEN
    CREATE ROLE etl_writer LOGIN PASSWORD 'local_etl_writer';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'dq_writer') THEN
    CREATE ROLE dq_writer LOGIN PASSWORD 'local_dq_writer';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'streamlit_reader') THEN
    CREATE ROLE streamlit_reader LOGIN PASSWORD 'local_streamlit_reader';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'grafana_reader') THEN
    CREATE ROLE grafana_reader LOGIN PASSWORD 'local_grafana_reader';
  END IF;
END $$;

-- Grants
GRANT USAGE ON SCHEMA serving, ops, rt TO etl_writer, dq_writer, streamlit_reader, grafana_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA serving, rt TO streamlit_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA ops TO grafana_reader, streamlit_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA serving GRANT SELECT ON TABLES TO streamlit_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA ops GRANT SELECT ON TABLES TO grafana_reader, streamlit_reader;

-- Ops tables used by pipeline (stubs — full DDL in later phases)
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
  run_id text PRIMARY KEY,
  batch_id text,
  stage text,
  status text,
  rows_read bigint,
  rows_written bigint,
  rows_rejected bigint,
  duration_ms bigint,
  started_at timestamptz DEFAULT now(),
  finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS ops.ingestion_watermarks (
  source_collection text PRIMARY KEY,
  last_watermark timestamptz,
  last_batch_id text,
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
  run_id text,
  batch_id text,
  layer text,
  table_name text,
  check_name text,
  check_type text,
  severity text,
  status text,
  failed_count bigint,
  total_count bigint,
  pass_pct double precision,
  checked_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.freshness_metrics (
  table_name text,
  freshness_seconds bigint,
  max_loaded_at timestamptz,
  checked_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.sla_events (
  table_name text,
  target_name text,
  target_seconds bigint,
  actual_seconds bigint,
  breached boolean,
  checked_at timestamptz DEFAULT now()
);
