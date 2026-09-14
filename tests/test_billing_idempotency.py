"""silver.billing idempotency — MERGE on transaction_id, not append.

Regression test for the production-readiness audit finding: the billing write
used mode("append"), so any re-run or backfill double-counted revenue. Runs
the real job function twice against a Delta-enabled session and asserts
stable row counts.

Uses its own Spark session with a temporary warehouse so the test never
touches the .spark/ local demo state.
"""

import pytest
from pyspark.sql import SparkSession

BILLING_SCHEMA = [
    "transaction_id",
    "user_id",
    "amount",
    "currency",
    "transaction_type",
    "transaction_timestamp",
    "plan_tier",
]

ROWS = [
    ("txn_1", "user_1", 9.99, "usd", "charge", "2026-01-01T10:00:00+00:00", "basic"),
    ("txn_2", "user_2", 15.99, "usd", "charge", "2026-01-02T10:00:00+00:00", "standard"),
    ("txn_3", "user_3", 19.99, "usd", "charge", "2026-01-03T10:00:00+00:00", "premium"),
    # duplicate transaction_id, later timestamp — dedupe keeps one
    ("txn_3", "user_3", 19.99, "usd", "charge", "2026-01-03T11:00:00+00:00", "premium"),
    # null transaction_id — quality gate quarantines it
    (None, "user_4", 5.00, "usd", "charge", "2026-01-04T10:00:00+00:00", "basic"),
]


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip

    warehouse = tmp_path_factory.mktemp("billing-test-warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("billing-idempotency-test")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def _seed_bronze(spark):
    # fresh temp warehouse — create the schemas the pipeline's ensure_schemas
    # would normally set up
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS silver")
    df = spark.createDataFrame(ROWS, schema=BILLING_SCHEMA)
    df.write.format("delta").mode("overwrite").saveAsTable("bronze.billing_transactions")
    spark.sql("DROP TABLE IF EXISTS silver.billing")


def test_billing_merge_is_idempotent(spark):
    from src.jobs.silver import silver_billing

    _seed_bronze(spark)

    silver_billing(spark)
    first = spark.table("silver.billing")
    assert first.count() == 3, "expected 3 rows: 4 valid txns deduped to 3, 1 quarantined"
    total = first.agg({"amount": "sum"}).collect()[0][0]
    assert total == pytest.approx(9.99 + 15.99 + 19.99), "duplicate txn_3 must not count twice"

    # the regression: re-running must not append duplicates
    silver_billing(spark)
    again = spark.table("silver.billing")
    assert again.count() == 3, "re-run appended rows — billing write is not idempotent"
    assert again.agg({"amount": "sum"}).collect()[0][0] == pytest.approx(
        9.99 + 15.99 + 19.99
    ), "re-run changed the revenue total"


def test_billing_quarantines_invalid_rows(spark):
    from pyspark.sql import functions as F

    from src.jobs.silver import silver_billing

    _seed_bronze(spark)
    silver_billing(spark)

    quarantine = spark.table("silver.quarantine")
    quarantined = quarantine.filter(F.col("transaction_id").isNull())
    assert quarantined.count() >= 1, "null transaction_id should land in quarantine"
    reasons = {r["quarantine_reason"] for r in quarantined.collect()}
    assert any("null_transaction_id" in r for r in reasons)
