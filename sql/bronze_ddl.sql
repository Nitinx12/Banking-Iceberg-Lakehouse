-- sql/bronze_ddl.sql — Bronze Iceberg DDL per Architecture 5.3 + 6.3/6.4 (Phase 1)
-- Catalog: banking (JdbcCatalog on postgres banking_dw), warehouse s3://banking-lakehouse/local/warehouse
-- Run via Spark session factory (jobs/common/spark.py) or spark-sql: spark.sql("CREATE TABLE banking.bronze.<table> ...")

-- Namespace
CREATE NAMESPACE IF NOT EXISTS banking.bronze;
CREATE NAMESPACE IF NOT EXISTS banking.ops;

-- Bronze tables — one per Mongo collection (10). Append only, raw _doc + lineage. Partition days(_ingested_at) for cheap replay.
-- Contract: Architecture 5.3

-- Template for incremental collections (customers, accounts, transactions, loans, cards, card_transactions, loan_payments, support_tickets, employees)
-- CREATE TABLE IF NOT EXISTS banking.bronze.<collection> (
--   _id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP,
--   _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING
-- ) USING iceberg
-- PARTITIONED BY (days(_ingested_at))
-- TBLPROPERTIES (
--   'format-version'='2', 'write.format.default'='parquet', 'write.parquet.compression-codec'='zstd',
--   'write.target-file-size-bytes'='134217728', 'write.delete.mode'='merge-on-read',
--   'history.expire.max-snapshot-age-ms'='604800000'
-- );

-- Generated DDL for 10 collections:

CREATE TABLE IF NOT EXISTS banking.bronze.customers (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.accounts (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.transactions (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.branches (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.loans (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.cards (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.card_transactions (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.loan_payments (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.support_tickets (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
CREATE TABLE IF NOT EXISTS banking.bronze.employees (_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet','write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728','write.delete.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000');
