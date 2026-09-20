"""jobs/transform/silver_card_transactions.py"""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
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

    w = Window.partitionBy("card_txn_id").orderBy(F.col("_source_ts").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    quarantine = deduped.filter(F.col("card_txn_id").isNull() | (F.col("amount") <= 0))
    clean = deduped.filter(F.col("card_txn_id").isNotNull() & (F.col("amount") > 0))
    clean.write.mode("append").saveAsTable("banking.silver.card_transactions")
    if quarantine.count() > 0:
        quarantine.withColumn("_dq_rule", F.lit("amount")).write.mode("append").saveAsTable(
            "banking.quarantine.card_transactions"
        )
    logger.info(f"silver_card_transactions: wrote {clean.count()} clean")
    return clean.count()
