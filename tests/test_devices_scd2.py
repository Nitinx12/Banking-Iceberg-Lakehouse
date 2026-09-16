"""Generic SCD2 tests for the devices dimension — mirrors tests/test_scd2.py.

Proves the apply_scd2_generic refactor in src/core/scd2.py works for a second
dimension (key_col + tracked_cols parameterized) while subscriptions behavior
is covered unchanged by tests/test_scd2.py.
"""

import pytest
from pyspark.sql import functions as F

from src.core.scd2 import apply_scd2_generic

pytestmark = pytest.mark.slow

DEVICE_SCHEMA = [
    "device_id",
    "user_id",
    "device_type",
    "os_family",
    "os_version",
    "app_version",
    "is_primary",
    "change_timestamp",
    "event_type",
]

TRACKED = [
    "user_id",
    "device_type",
    "os_family",
    "os_version",
    "app_version",
    "is_primary",
]


def apply_devices_scd2(target_df, cdc_df):
    return apply_scd2_generic(
        target_df, cdc_df, key_col="device_id", tracked_cols=TRACKED
    )


def test_devices_scd2_basic_insert_and_update(spark):
    cdc = spark.createDataFrame(
        [
            (
                "dev_1",
                "user_1",
                "tv",
                "tizen",
                "1.0",
                "3.1.0",
                True,
                "2024-01-01T00:00:00+00:00",
                "insert",
            ),
            (
                "dev_1",
                "user_1",
                "tv",
                "tizen",
                "2.0",
                "3.2.0",
                True,
                "2024-03-01T00:00:00+00:00",
                "update",
            ),
        ],
        schema=DEVICE_SCHEMA,
    )
    result = apply_devices_scd2(None, cdc)
    assert result.count() == 2
    # only one current per key
    assert result.filter(F.col("is_current") == True).count() == 1
    # first row closed, carries the pre-update os_version
    first = result.filter(
        F.col("effective_date") == F.to_timestamp(F.lit("2024-01-01T00:00:00+00:00"))
    ).collect()[0]
    assert first["end_date"] is not None
    assert first["is_current"] is False
    assert first["os_version"] == "1.0"
    # current row has the bumped version
    current = result.filter(F.col("is_current") == True).collect()[0]
    assert current["os_version"] == "2.0"


def test_devices_scd2_delete_closes_without_new_current(spark):
    cdc = spark.createDataFrame(
        [
            (
                "dev_2",
                "user_2",
                "mobile",
                "android",
                "14.0",
                "3.1.0",
                False,
                "2024-01-01T00:00:00+00:00",
                "insert",
            ),
            (
                "dev_2",
                "user_2",
                "mobile",
                "android",
                "14.0",
                "3.1.0",
                False,
                "2024-02-01T00:00:00+00:00",
                "delete",
            ),
        ],
        schema=DEVICE_SCHEMA,
    )
    result = apply_devices_scd2(None, cdc)
    assert result.filter(F.col("is_current") == True).count() == 0


def test_devices_scd2_idempotent_repeat_run(spark):
    """DoD requirement: same batch twice -> same row count."""
    cdc = spark.createDataFrame(
        [
            (
                "dev_3",
                "user_3",
                "web",
                "windows",
                "11.0",
                "3.0.0",
                True,
                "2024-01-01T00:00:00+00:00",
                "insert",
            ),
            (
                "dev_3",
                "user_3",
                "web",
                "windows",
                "11.1",
                "3.1.0",
                True,
                "2024-04-01T00:00:00+00:00",
                "update",
            ),
        ],
        schema=DEVICE_SCHEMA,
    )
    first = apply_devices_scd2(None, cdc)
    cnt1 = first.count()
    second = apply_devices_scd2(first, cdc)
    cnt2 = second.count()
    assert cnt1 == cnt2, f"idempotency failed: {cnt1} != {cnt2}"


def test_devices_scd2_unsorted_input_handled(spark):
    # intentionally out-of-order
    cdc = spark.createDataFrame(
        [
            (
                "dev_4",
                "user_4",
                "tv",
                "webos",
                "2.0",
                "3.2.0",
                True,
                "2024-03-01T00:00:00+00:00",
                "update",
            ),
            (
                "dev_4",
                "user_4",
                "tv",
                "webos",
                "1.0",
                "3.1.0",
                True,
                "2024-01-01T00:00:00+00:00",
                "insert",
            ),
            (
                "dev_4",
                "user_4",
                "tv",
                "webos",
                "1.5",
                "3.1.5",
                True,
                "2024-02-01T00:00:00+00:00",
                "update",
            ),
        ],
        schema=DEVICE_SCHEMA,
    )
    result = apply_devices_scd2(None, cdc)
    rows = result.orderBy("effective_date").collect()
    assert rows[0]["os_version"] == "1.0"
    assert rows[1]["os_version"] == "1.5"
    assert rows[2]["os_version"] == "2.0"
    assert rows[2]["is_current"] is True
