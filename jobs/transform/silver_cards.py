"""jobs/transform/silver_cards.py"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from jobs.common.logging import get_logger
from jobs.common.spark import get_spark

logger = get_logger("silver_cards")
SCHEMA = StructType(
    [
        StructField("card_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("account_id", IntegerType(), True),
        StructField("card_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def run(batch_id: str = None):
    spark = get_spark("silver_cards")
    bronze = spark.table("banking.bronze.cards")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )
    from pyspark.sql.window import Window

    w = Window.partitionBy("card_id").orderBy(F.col("_source_ts").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    quarantine = deduped.filter(F.col("card_id").isNull())
    clean = deduped.filter(F.col("card_id").isNotNull())
    clean.write.mode("append").saveAsTable("banking.silver.cards")
    if quarantine.count() > 0:
        quarantine.withColumn("_dq_rule", F.lit("null")).write.mode("append").saveAsTable(
            "banking.quarantine.cards"
        )
    logger.info(f"silver_cards: wrote {clean.count()} clean")
    return clean.count()
