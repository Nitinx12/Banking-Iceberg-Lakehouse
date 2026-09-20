"""jobs/common/config.py — env-driven config for Phase 1 (Architecture 4,5,6,8)."""

import os

from dotenv import load_dotenv

load_dotenv()


def env(k, default=None):
    return os.getenv(k, default)


# Core
ENVIRONMENT = env("ENVIRONMENT", "local")
BUSINESS_TIMEZONE = env("BUSINESS_TIMEZONE", "Asia/Kolkata")

# Postgres / JDBC catalog
POSTGRES_HOST = env("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(env("POSTGRES_PORT", "5432"))
POSTGRES_CATALOG_DB = env("POSTGRES_CATALOG_DB", "iceberg_catalog")
POSTGRES_WAREHOUSE_DB = env("POSTGRES_WAREHOUSE_DB", "banking_dw")
ICEBERG_CATALOG_NAME = env("ICEBERG_CATALOG_NAME", "banking")
ICEBERG_CATALOG_URI = env(
    "ICEBERG_CATALOG_URI",
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_CATALOG_DB}",
)
ICEBERG_CATALOG_WAREHOUSE = env(
    "ICEBERG_CATALOG_WAREHOUSE", "s3://banking-lakehouse/local/warehouse"
)

# S3 / MinIO
S3_ENDPOINT = env("S3_ENDPOINT", "http://minio:9000")
S3_REGION = env("S3_REGION", "us-east-1")
S3_BUCKET = env("S3_BUCKET", "banking-lakehouse")
AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY", "")

# Mongo
MONGO_URI = env(
    "MONGO_URI", "mongodb://admin:password@mongo:27017/banking?replicaSet=rs0&authSource=admin"
)
MONGO_HOST = env("MONGO_HOST", "mongo")
MONGO_PORT = int(env("MONGO_PORT", "27017"))

# Ingestion
INGEST_OVERLAP_MINUTES = int(env("INGEST_OVERLAP_MINUTES", "10"))
INGEST_BATCH_ID_PREFIX = env("INGEST_BATCH_ID_PREFIX", "local")
INGEST_COLLECTIONS = [
    c.strip()
    for c in env(
        "INGEST_COLLECTIONS",
        "customers,accounts,transactions,branches,loans,cards,card_transactions,loan_payments,support_tickets,employees",
    ).split(",")
    if c.strip()
]

# Collections that are full refresh (small)
FULL_REFRESH_COLLECTIONS = {"branches"}

# Watermark field per collection — all created_at except branches (profiled docs/profiling.md)
WATERMARK_FIELDS = {
    "customers": "created_at",
    "accounts": "created_at",
    "transactions": "created_at",
    "branches": None,
    "loans": "created_at",
    "cards": "created_at",
    "card_transactions": "created_at",
    "loan_payments": "created_at",
    "support_tickets": "created_at",
    "employees": "created_at",
}

# Bronze Iceberg properties (Architecture 6.3, 6.4)
BRONZE_TABLE_PROPS = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.target-file-size-bytes": "134217728",
    "write.delete.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.merge.mode": "merge-on-read",
    "history.expire.max-snapshot-age-ms": "604800000",
}
