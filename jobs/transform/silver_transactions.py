"""jobs/transform/silver_transactions.py — Silver typed/deduped (Architecture 7.3)."""

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

logger = get_logger("silver_transactions")

TXN_SCHEMA = StructType(
    [
        StructField("transaction_id", IntegerType(), False),
        StructField("account_id", IntegerType(), True),
        StructField("txn_type", StringType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("channel", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def run(batch_id: str = None):
    spark = get_spark("silver_transactions")
    bronze = spark.table("banking.bronze.transactions")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), TXN_SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )
    from pyspark.sql.window import Window

    w = Window.partitionBy("transaction_id").orderBy(
        F.col("_source_ts").desc(), F.col("_id").desc()
    )
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    # audit cols per 7.3
    deduped = (
        deduped.withColumn("silver_loaded_at", F.current_timestamp())
        .withColumn("_bronze_batch_id", F.col("_batch_id"))
        .withColumn("_bronze_doc_hash", F.col("_doc_hash"))
    )
    # domain checks: amount >0, valid types
    quarantine = deduped.filter((F.col("transaction_id").isNull()) | (F.col("amount") <= 0))
    clean = deduped.filter(F.col("transaction_id").isNotNull() & (F.col("amount") > 0))
    clean.createOrReplaceTempView("clean_transactions")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    # ensure table exists (schema from clean)
    if not spark.catalog.tableExists("banking.silver.transactions"):
        clean.limit(0).write.mode("append").saveAsTable("banking.silver.transactions")
    spark.sql("""
        MERGE INTO banking.silver.transactions t USING clean_transactions s
        ON t.transaction_id = s.transaction_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    qcnt = quarantine.count()
    if qcnt > 0:
        quarantine.withColumn("_dq_rule", F.lit("amount_check")).withColumn(
            "_quarantined_at", F.current_timestamp()
        ).write.mode("append").saveAsTable("banking.quarantine.transactions")
    cnt = clean.count()
    logger.info(f"silver_transactions: wrote {cnt} clean, {qcnt} quarantine")
    return cnt
