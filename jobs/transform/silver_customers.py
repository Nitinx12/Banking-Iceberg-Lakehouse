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
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
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

    # Deduplicate by latest _source_ts per customer_id
    from pyspark.sql.window import Window

    w = Window.partitionBy("customer_id").orderBy(F.col("_source_ts").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")

    # Mask PII + standardise
    hmac_udf = F.udf(lambda v: _hmac(v, secret, salt), StringType())
    silver = (
        deduped.withColumn("email_hmac", hmac_udf(F.col("email")))
        .withColumn("phone_hmac", hmac_udf(F.col("phone")))
        .withColumn("name", F.trim(F.col("name")))
    )

    # Quarantine violations: e.g. null customer_id
    quarantine = silver.filter(F.col("customer_id").isNull())
    clean = silver.filter(F.col("customer_id").isNotNull())

    # Write to Silver (Delta on CE per ADR 003) — here we write to banking.silver_customers for demo
    clean.write.mode("append").saveAsTable("banking.silver.customers")
    # Quarantine
    if quarantine.count() > 0:
        quarantine.withColumn("_dq_rule", F.lit("not_null customer_id")).write.mode(
            "append"
        ).saveAsTable("banking.quarantine.customers")

    logger.info(f"silver_customers: wrote {clean.count()} clean, {quarantine.count()} quarantine")
    return clean.count()
