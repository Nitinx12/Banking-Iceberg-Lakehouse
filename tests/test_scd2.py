"""SCD2 tests — including idempotency repeat-run required by DoD."""

import pytest
from pyspark.sql import functions as F

from src.core.scd2 import apply_scd2

pytestmark = pytest.mark.slow


def test_scd2_basic_insert_and_update(spark):
    cdc = spark.createDataFrame(
        [
            ("sub_1", "user_1", "basic", "active", "2024-01-01T00:00:00+00:00", "insert", None),
            ("sub_1", "user_1", "premium", "active", "2024-02-01T00:00:00+00:00", "update", "basic"),
        ],
        schema=["subscription_id", "user_id", "plan_tier", "status", "change_timestamp", "event_type", "previous_plan_tier"],
    )
    result = apply_scd2(None, cdc)
    assert result.count() == 2
    # only one current per key
    assert result.filter(F.col("is_current") == True).count() == 1
    # first row closed
    first = result.filter(F.col("effective_date") == F.to_timestamp(F.lit("2024-01-01T00:00:00+00:00"))).collect()[0]
    assert first["end_date"] is not None
    assert first["is_current"] is False


def test_scd2_delete_closes_without_new_current(spark):
    cdc = spark.createDataFrame(
        [
            ("sub_2", "user_2", "basic", "active", "2024-01-01T00:00:00+00:00", "insert", None),
            ("sub_2", "user_2", "basic", "canceled", "2024-02-01T00:00:00+00:00", "delete", "basic"),
        ],
        schema=["subscription_id", "user_id", "plan_tier", "status", "change_timestamp", "event_type", "previous_plan_tier"],
    )
    result = apply_scd2(None, cdc)
    # after delete, no current
    assert result.filter(F.col("is_current") == True).count() == 0


def test_scd2_idempotent_repeat_run(spark):
    """DoD requirement: same batch twice -> same row count."""
    cdc = spark.createDataFrame(
        [
            ("sub_3", "user_3", "basic", "active", "2024-01-01T00:00:00+00:00", "insert", None),
            ("sub_3", "user_3", "standard", "active", "2024-02-01T00:00:00+00:00", "update", "basic"),
        ],
        schema=["subscription_id", "user_id", "plan_tier", "status", "change_timestamp", "event_type", "previous_plan_tier"],
    )
    first = apply_scd2(None, cdc)
    cnt1 = first.count()
    second = apply_scd2(first, cdc)
    cnt2 = second.count()
    assert cnt1 == cnt2, f"idempotency failed: {cnt1} != {cnt2}"


def test_scd2_unsorted_input_handled(spark):
    # intentionally out-of-order
    cdc = spark.createDataFrame(
        [
            ("sub_4", "user_4", "premium", "active", "2024-03-01T00:00:00+00:00", "update", "basic"),
            ("sub_4", "user_4", "basic", "active", "2024-01-01T00:00:00+00:00", "insert", None),
            ("sub_4", "user_4", "standard", "active", "2024-02-01T00:00:00+00:00", "update", "basic"),
        ],
        schema=["subscription_id", "user_id", "plan_tier", "status", "change_timestamp", "event_type", "previous_plan_tier"],
    )
    result = apply_scd2(None, cdc)
    rows = result.orderBy("effective_date").collect()
    assert rows[0]["plan_tier"] == "basic"
    assert rows[1]["plan_tier"] == "standard"
    assert rows[2]["plan_tier"] == "premium"
    assert rows[2]["is_current"] is True