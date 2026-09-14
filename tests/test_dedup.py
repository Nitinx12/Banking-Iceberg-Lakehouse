"""Dedup / idempotency tests for watch_events."""

from src.transformations import dedupe_on_key


def test_dedupe_on_event_id(spark):
    df = spark.createDataFrame(
        [
            ("evt_1", "user_1", "2024-01-01T00:00:00+00:00"),
            ("evt_1", "user_1", "2024-01-02T00:00:00+00:00"),
            ("evt_2", "user_2", "2024-01-01T00:00:00+00:00"),
        ],
        schema=["event_id", "user_id", "event_timestamp"],
    )
    result = dedupe_on_key(df, ["event_id"], order_by="event_timestamp")
    assert result.count() == 2
    # keeps latest
    row = result.filter("event_id='evt_1'").collect()[0]
    assert "2024-01-02" in str(row["event_timestamp"])


def test_repeat_batch_idempotent(spark):
    df = spark.createDataFrame(
        [("evt_1", "user_1", "2024-01-01T00:00:00+00:00")],
        schema=["event_id", "user_id", "event_timestamp"],
    )
    d1 = dedupe_on_key(df.union(df), ["event_id"])
    assert d1.count() == 1
