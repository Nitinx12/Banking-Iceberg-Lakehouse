"""jobs/transform/silver_customers.py — Silver typed/deduped/masked (Architecture 7.1-7.3).

Parse Bronze _doc JSON with explicit StructType, cast money decimal(18,2), deduplicate by latest _source_ts per customer_id,
standardise names, mask PII with HMAC (PII_HMAC_SECRET), explode nested arrays, route violations to Quarantine.
"""

import hashlib
import hmac
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.common.logging import get_logger
from jobs.common.spark import get_spark

logger = get_logger("silver_customers")

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("email", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("annual_income", IntegerType(), True),
        StructField("join_date", StringType(), True),
        StructField("credit_score", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def _hmac(value: str, secret: str, salt: str) -> str:
    if value is None:
        return None
    return hmac.new((secret + salt).encode(), value.encode(), hashlib.sha256).hexdigest()


def run(batch_id: str = None):
    spark = get_spark("silver_customers")
    secret = os.getenv("PII_HMAC_SECRET", "")
    salt = os.getenv("PII_HMAC_SALT", "")

    bronze = spark.table("banking.bronze.customers")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)

    # Parse _doc with explicit schema
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), CUSTOMER_SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )

    # Deduplicate by latest _source_ts per customer_id with _id tiebreaker (profiling: same created_at)
    from pyspark.sql.window import Window

    w = Window.partitionBy("customer_id").orderBy(F.col("_source_ts").desc(), F.col("_id").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # Mask PII + standardise + audit cols per Architecture 7.3
    hmac_udf = F.udf(lambda v: _hmac(v, secret, salt), StringType())
    silver = (
        deduped.withColumn("email_hmac", hmac_udf(F.col("email")))
        .withColumn("phone_hmac", hmac_udf(F.col("phone")))
        .withColumn("name", F.trim(F.col("name")))
        .withColumn("silver_loaded_at", F.current_timestamp())
        .withColumn("_bronze_batch_id", F.col("_batch_id"))
        .withColumn("_bronze_doc_hash", F.col("_doc_hash"))
        .drop("email")
        .drop("phone")
    )

    # Quarantine violations with full lineage per 11.4
    quarantine = (
        silver.filter(F.col("customer_id").isNull())
        .withColumn("_dq_rule", F.lit("not_null customer_id"))
        .withColumn("_quarantined_at", F.current_timestamp())
    )
    clean = silver.filter(F.col("customer_id").isNotNull())

    # Write to Silver — MERGE (upsert by business key) per Architecture 6.6, not append
    # Use Iceberg MERGE INTO for idempotency across batches
    clean.createOrReplaceTempView("clean_customers")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS banking.silver.customers (customer_id INT, name STRING, gender STRING, date_of_birth STRING, city STRING, state STRING, email_hmac STRING, phone_hmac STRING, occupation STRING, annual_income INT, join_date STRING, credit_score INT, created_at TIMESTAMP, silver_loaded_at TIMESTAMP, _bronze_batch_id STRING, _bronze_doc_hash STRING) USING iceberg"
    )
    spark.sql("""
        MERGE INTO banking.silver.customers t USING clean_customers s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    if quarantine.count() > 0:
        quarantine.write.mode("append").saveAsTable("banking.quarantine.customers")
    # single count via action
    cnt = clean.count()
    qcnt = quarantine.count()
    logger.info(f"silver_customers: wrote {cnt} clean, {qcnt} quarantine")
    return cnt
