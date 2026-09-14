"""Data validations — each GX expectation has a failing-data test (quarantine path)."""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.core.quality_checks import check_billing, check_watch_events


def test_gx_watch_events_null_rejected(spark):
    schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("content_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("watch_duration_seconds", IntegerType(), True),
            StructField("device_type", StringType(), True),
            StructField("session_id", StringType(), True),
        ]
    )
    df = spark.createDataFrame([("e1", "u1", "c1", "play", "2024-01-01T00:00:00+00:00", 100, None, "s1")], schema=schema)
    res = check_watch_events(df)
    assert res.fail_count == 1
    assert any("null" in k for k in res.reasons)


def test_gx_watch_events_duplicate_rejected(spark):
    df = spark.createDataFrame(
        [("e1", "u1", "c1", "play", "2024-01-01T00:00:00+00:00", 100, "tv", "s1"), ("e1", "u1", "c1", "play", "2024-01-02T00:00:00+00:00", 100, "tv", "s1")],
        schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
    )
    res = check_watch_events(df)
    assert res.fail_count >= 1


def test_gx_watch_events_negative_duration(spark):
    df = spark.createDataFrame(
        [("e2", "u1", "c1", "play", "2024-01-01T00:00:00+00:00", -5, "tv", "s1")],
        schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
    )
    res = check_watch_events(df)
    assert res.fail_count >= 1


def test_gx_watch_events_future_timestamp(spark):
    df = spark.createDataFrame(
        [("e3", "u1", "c1", "play", "2099-01-01T00:00:00+00:00", 100, "tv", "s1")],
        schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
    )
    res = check_watch_events(df)
    assert res.fail_count >= 1


def test_gx_billing_null_amount(spark):
    from pyspark.sql.types import DoubleType

    schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("transaction_timestamp", StringType(), True),
            StructField("plan_tier", StringType(), True),
        ]
    )
    df = spark.createDataFrame([("t1", "u1", None, "USD", "charge", "2024-01-01T00:00:00+00:00", "basic")], schema=schema)
    res = check_billing(df)
    assert res.fail_count == 1


def test_gx_valid_watch_events_pass(spark):
    from datetime import UTC, datetime, timedelta

    recent = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    df = spark.createDataFrame(
        [("e9", "u1", "c1", "play", recent, 100, "tv", "s1")],
        schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
    )
    res = check_watch_events(df)
    assert res.pass_count == 1 and res.fail_count == 0
