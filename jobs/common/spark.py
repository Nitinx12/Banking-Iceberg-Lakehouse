"""jobs/common/spark.py - Spark session factory for Iceberg JDBC + MinIO (Architecture 6.1, Phase 1).

CE path: Iceberg-everywhere via JDBC catalog `banking` on postgres + S3A MinIO (ADR 003 amended 2026-09-21).
Silver/Gold stay USING iceberg on CE; Delta fallback only for paid Databricks Unity Catalog if required.
"""

import os
import sys

from pyspark.sql import SparkSession

from jobs.common import config as cfg


def get_spark(app_name="banking_bronze") -> SparkSession:
    # Windows: ensure workers use venv python, not Store stub
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # Iceberg + S3A dependencies are provided via --packages at submission.
    # Pinned to iceberg 1.5.2 + hadoop-aws 3.3.4 to match Spark 3.5.5 bundled Hadoop 3.3.4 (BulkDelete compat).
    # For local `uv run` we rely on pyspark's built-in Hadoop; S3A creds via env.
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{cfg.ICEBERG_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            f"spark.sql.catalog.{cfg.ICEBERG_CATALOG_NAME}.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .config(f"spark.sql.catalog.{cfg.ICEBERG_CATALOG_NAME}.uri", cfg.ICEBERG_CATALOG_URI)
        .config(
            f"spark.sql.catalog.{cfg.ICEBERG_CATALOG_NAME}.warehouse", cfg.ICEBERG_CATALOG_WAREHOUSE
        )
        .config(
            f"spark.sql.catalog.{cfg.ICEBERG_CATALOG_NAME}.jdbc.user",
            cfg.env("POSTGRES_USER", "postgres"),
        )
        .config(
            f"spark.sql.catalog.{cfg.ICEBERG_CATALOG_NAME}.jdbc.password",
            cfg.env("POSTGRES_PASSWORD", ""),
        )
        # JDBC catalog without view support blocks CREATE OR REPLACE TABLE / temp views on
        # pre-existing namespaces (UnsupportedOperationException: jdbc.schema-version=V1)
        .config(f"spark.sql.catalog.{cfg.ICEBERG_CATALOG_NAME}.jdbc.schema-version", "V1")
        # S3A / MinIO - for host runs S3_ENDPOINT must be localhost:9000 (not minio:9000)
        .config("spark.hadoop.fs.s3a.endpoint", cfg.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", cfg.AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.aws.region", cfg.S3_REGION)
        # Bronze partitioning default (Architecture 6.4). Adaptive AQE coalesces the
        # 16 shuffle partitions down for small local data, so dev runs are fast while
        # prod-scale runs still get real parallelism.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m")
        .config("spark.sql.shuffle.partitions", cfg.env("SPARK_SHUFFLE_PARTITIONS", "16"))
        # Windows: Spark workers must use venv python, not Store stub "python"
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
    )
    # JDBC driver for pg catalog - align Hadoop to Spark's bundled 3.3.4 (BulkDelete missing in 3.3 breaks 3.4/1.6 combo)
    # Stable connection: if jars/*.jar present (from `make jars`) or SPARK_JARS env set, use local spark.jars
    # so the run doesn't need Maven Central/Ivy on every invocation (offline-friendly).
    import glob as _glob

    local_jars = os.getenv("SPARK_JARS", "")
    if not local_jars:
        # auto-pick jars/*.jar if downloaded via scripts/sh/download_jars.sh
        found = _glob.glob("jars/*.jar")
        if found:
            local_jars = ",".join(found)
    if local_jars:
        builder = builder.config("spark.jars", local_jars)
    else:
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.780",
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # suppress Ivy resolver noise on first run - jars already cached after first resolve
    return spark
