"""jobs/transform/silver_loan_payments.py"""

from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StructField, StructType, TimestampType

from jobs.common.logging import get_logger
from jobs.common.spark import get_spark

logger = get_logger("silver_loan_payments")
SCHEMA = StructType(
    [
        StructField("payment_id", IntegerType(), False),
        StructField("loan_id", IntegerType(), True),
        StructField("amount_paid", DecimalType(18, 2), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def run(batch_id: str = None):
    spark = get_spark("silver_loan_payments")
    bronze = spark.table("banking.bronze.loan_payments")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )
    from pyspark.sql.window import Window

    w = Window.partitionBy("payment_id").orderBy(F.col("_source_ts").desc(), F.col("_id").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    # audit cols per 7.3
    deduped = (
        deduped.withColumn("silver_loaded_at", F.current_timestamp())
        .withColumn("_bronze_batch_id", F.col("_batch_id"))
        .withColumn("_bronze_doc_hash", F.col("_doc_hash"))
    )
    quarantine = deduped.filter(F.col("payment_id").isNull() | (F.col("amount_paid") <= 0))
    clean = deduped.filter(F.col("payment_id").isNotNull() & (F.col("amount_paid") > 0))
    clean.createOrReplaceTempView("clean_loan_payments")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    # ensure table exists (schema from clean)
    if not spark.catalog.tableExists("banking.silver.loan_payments"):
        clean.limit(0).write.mode("append").saveAsTable("banking.silver.loan_payments")
    spark.sql("""
        MERGE INTO banking.silver.loan_payments t USING clean_loan_payments s
        ON t.payment_id = s.payment_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    if quarantine.count() > 0:  # single count
        quarantine.withColumn("_dq_rule", F.lit("amount")).write.mode("append").saveAsTable(
            "banking.quarantine.loan_payments"
        )
    logger.info(f"silver_loan_payments: wrote {clean.count()} clean")
    return clean.count()
