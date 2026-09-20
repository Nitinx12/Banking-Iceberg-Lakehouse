"""jobs/transform/silver_loans.py — Silver loans."""

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from jobs.common.logging import get_logger
from jobs.common.spark import get_spark

logger = get_logger("silver_loans")
SCHEMA = StructType(
    [
        StructField("loan_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("branch_id", IntegerType(), True),
        StructField("loan_type", StringType(), True),
        StructField("loan_amount", DecimalType(18, 2), True),
        StructField("interest_rate", DoubleType(), True),
        StructField("term_months", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)


def run(batch_id: str = None):
    spark = get_spark("silver_loans")
    bronze = spark.table("banking.bronze.loans")
    if batch_id:
        bronze = bronze.filter(F.col("_batch_id") == batch_id)
    parsed = bronze.withColumn("parsed", F.from_json(F.col("_doc"), SCHEMA)).select(
        "_id", "_batch_id", "_source_ts", "_ingested_at", "_doc_hash", "parsed.*"
    )
    from pyspark.sql.window import Window

    w = Window.partitionBy("loan_id").orderBy(F.col("_source_ts").desc(), F.col("_id").desc())
    deduped = parsed.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    # audit cols per 7.3
    deduped = (
        deduped.withColumn("silver_loaded_at", F.current_timestamp())
        .withColumn("_bronze_batch_id", F.col("_batch_id"))
        .withColumn("_bronze_doc_hash", F.col("_doc_hash"))
    )
    quarantine = deduped.filter(F.col("loan_id").isNull() | (F.col("loan_amount") <= 0))
    clean = deduped.filter(F.col("loan_id").isNotNull() & (F.col("loan_amount") > 0))
    clean.createOrReplaceTempView("clean_loans")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.silver")
    # ensure table exists (schema from clean)
    if not spark.catalog.tableExists("banking.silver.loans"):
        clean.limit(0).write.mode("append").saveAsTable("banking.silver.loans")
    spark.sql("""
        MERGE INTO banking.silver.loans t USING clean_loans s
        ON t.loan_id = s.loan_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    if quarantine.count() > 0:  # single count
        quarantine.withColumn("_dq_rule", F.lit("amount")).write.mode("append").saveAsTable(
            "banking.quarantine.loans"
        )
    logger.info(f"silver_loans: wrote {clean.count()} clean")
    return clean.count()
