"""jobs/transform/silver_card_transactions.py"""

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

logger = get_logger("silver_card_transactions")
SCHEMA = StructType(
    [
        StructField("card_txn_id", IntegerType(), False),
        StructField("card_id", IntegerType(), True),
        StructField("amount", DecimalType(18, 2), True),
        StructField("is_fraud", IntegerType(), True),
        StructField("txn_date", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def run(batch_id: str = None):
    spark = get_spark("silver_card_transactions")
    bronze = spark.table("banking.bronze.card_transactions")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )
    from pyspark.sql.window import Window

    w = Window.partitionBy("card_txn_id").orderBy(F.col("_source_ts").desc(), F.col("_id").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    # audit cols per 7.3
    deduped = (
        deduped.withColumn("silver_loaded_at", F.current_timestamp())
        .withColumn("_bronze_batch_id", F.col("_batch_id"))
        .withColumn("_bronze_doc_hash", F.col("_doc_hash"))
    )
    quarantine = deduped.filter(F.col("card_txn_id").isNull() | (F.col("amount") <= 0))
    clean = deduped.filter(F.col("card_txn_id").isNotNull() & (F.col("amount") > 0))
    clean.createOrReplaceTempView("clean_card_transactions")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    # ensure table exists (schema from clean)
    if not spark.catalog.tableExists("banking.silver.card_transactions"):
        clean.limit(0).write.mode("append").saveAsTable("banking.silver.card_transactions")
    spark.sql("""
        MERGE INTO banking.silver.card_transactions t USING clean_card_transactions s
        ON t.card_txn_id = s.card_txn_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    if quarantine.count() > 0:  # single count
        quarantine.withColumn("_dq_rule", F.lit("amount")).write.mode("append").saveAsTable(
            "banking.quarantine.card_transactions"
        )
    logger.info(f"silver_card_transactions: wrote {clean.count()} clean")
    return clean.count()
