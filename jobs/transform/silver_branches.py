"""jobs/transform/silver_branches.py — Silver full refresh (150 rows)."""

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from jobs.common.logging import get_logger
from jobs.common.spark import get_spark

logger = get_logger("silver_branches")
SCHEMA = StructType(
    [
        StructField("branch_id", IntegerType(), False),
        StructField("branch_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("ifsc_code", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def run(batch_id: str = None):
    spark = get_spark("silver_branches")
    bronze = spark.table("banking.bronze.branches")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )
    # full refresh: dedup by branch_id
    from pyspark.sql.window import Window

    w = Window.partitionBy("branch_id").orderBy(F.col("_source_ts").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    quarantine = deduped.filter(F.col("branch_id").isNull())
    clean = deduped.filter(F.col("branch_id").isNotNull())
    # overwrite for full refresh
    clean.write.mode("overwrite").saveAsTable("banking.silver.branches")
    if quarantine.count() > 0:
        quarantine.withColumn("_dq_rule", F.lit("not_null")).write.mode("append").saveAsTable(
            "banking.quarantine.branches"
        )
    logger.info(f"silver_branches: wrote {clean.count()} clean")
    return clean.count()
