"""Quality gate tests — each messiness type has a catching check."""

from datetime import UTC

from src.core.quality_checks import check_watch_events


def test_duplicate_event_id_quarantined(spark):
    df = spark.createDataFrame(
        [
            ("evt_1", "user_1", "ct_1", "play", "2024-01-01T00:00:00+00:00", 100, "tv", "s1"),
            ("evt_1", "user_1", "ct_1", "play", "2024-01-02T00:00:00+00:00", 100, "tv", "s1"),
        ],
        schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
    )
    result = check_watch_events(df)
    assert result.fail_count >= 1
    reasons = [r["quarantine_reason"] for r in result.quarantined.select("quarantine_reason").collect()]
    assert any("duplicate" in (r or "") for r in reasons)


def test_null_device_type_quarantined(spark):
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

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
    df = spark.createDataFrame(
        [("evt_2", "user_1", "ct_1", "play", "2024-01-01T00:00:00+00:00", 100, None, "s1")],
        schema=schema,
    )
    result = check_watch_events(df)
    assert result.fail_count == 1


def test_future_timestamp_quarantined(spark):
    df = spark.createDataFrame(
        [("evt_3", "user_1", "ct_1", "play", "2099-01-01T00:00:00+00:00", 100, "tv", "s1")],
        schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
    )
    result = check_watch_events(df)
    assert result.fail_count == 1


def test_late_arriving_flagged(spark):
    df = spark.createDataFrame(
        [("evt_4", "user_1", "ct_1", "play", "2020-01-01T00:00:00+00:00", 100, "tv", "s1")],
        schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
    )
    result = check_watch_events(df)
    # late threshold is 7 days behind now -> 2020 is late
    assert result.fail_count == 1


def test_valid_record_passes(spark):
    # valid recent record should pass

    from datetime import datetime, timedelta

    recent = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    df2 = spark.createDataFrame(
        [("evt_5", "user_1", "ct_1", "play", recent, 100, "tv", "s1")],
        schema=["event_id", "user_id", "content_id", "event_type", "event_timestamp", "watch_duration_seconds", "device_type", "session_id"],
    )
    result = check_watch_events(df2)
    assert result.pass_count == 1
    assert result.fail_count == 0
