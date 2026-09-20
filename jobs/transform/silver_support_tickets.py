"""jobs/transform/silver_support_tickets.py"""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from jobs.common.logging import get_logger
from jobs.common.spark import get_spark

logger = get_logger("silver_support_tickets")
SCHEMA = StructType(
    [
        StructField("ticket_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("satisfaction_score", IntegerType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def run(batch_id: str = None):
    spark = get_spark("silver_support_tickets")
    bronze = spark.table("banking.bronze.support_tickets")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )
    from pyspark.sql.window import Window

    w = Window.partitionBy("ticket_id").orderBy(F.col("_source_ts").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    quarantine = deduped.filter(F.col("ticket_id").isNull())
    clean = deduped.filter(F.col("ticket_id").isNotNull())
    clean.write.mode("append").saveAsTable("banking.silver.support_tickets")
    if quarantine.count() > 0:
        quarantine.withColumn("_dq_rule", F.lit("null")).write.mode("append").saveAsTable(
            "banking.quarantine.support_tickets"
        )
    logger.info(f"silver_support_tickets: wrote {clean.count()} clean")
    return clean.count()
