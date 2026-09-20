"""jobs/transform/silver_accounts.py — Silver typed/deduped (Architecture 7.3)."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.common.logging import get_logger
from jobs.common.spark import get_spark

logger = get_logger("silver_accounts")

ACCOUNT_SCHEMA = StructType(
    [
        StructField("account_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("branch_id", IntegerType(), True),
        StructField("account_type", StringType(), True),
        StructField("balance", DecimalType(18, 2), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def run(batch_id: str = None):
    spark = get_spark("silver_accounts")
    bronze = spark.table("banking.bronze.accounts")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), ACCOUNT_SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )
    from pyspark.sql.window import Window

    w = Window.partitionBy("account_id").orderBy(F.col("_source_ts").desc(), F.col("_id").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    # audit cols per 7.3
    deduped = (
        deduped.withColumn("silver_loaded_at", F.current_timestamp())
        .withColumn("_bronze_batch_id", F.col("_batch_id"))
        .withColumn("_bronze_doc_hash", F.col("_doc_hash"))
    )
    quarantine = deduped.filter(F.col("account_id").isNull() | F.col("balance").isNull())
    clean = deduped.filter(F.col("account_id").isNotNull() & F.col("balance").isNotNull())
    clean.createOrReplaceTempView("clean_accounts")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    # ensure table exists (schema from clean)
    if not spark.catalog.tableExists("banking.silver.accounts"):
        clean.limit(0).write.mode("append").saveAsTable("banking.silver.accounts")
    spark.sql("""
        MERGE INTO banking.silver.accounts t USING clean_accounts s
        ON t.account_id = s.account_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    if quarantine.count() > 0:  # single count
        quarantine.withColumn("_dq_rule", F.lit("not_null")).write.mode("append").saveAsTable(
            "banking.quarantine.accounts"
        )
    logger.info(f"silver_accounts: wrote {clean.count()} clean, {quarantine.count()} quarantine")
    return clean.count()
