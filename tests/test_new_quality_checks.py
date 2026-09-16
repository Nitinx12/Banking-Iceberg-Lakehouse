"""Quality gate tests for the new tables — each messiness type has a catching check.

Mirrors tests/test_quality_checks.py: one deliberately bad record per check.
"""

import json
from datetime import UTC, datetime, timedelta

import pytest

from src.core.quality_checks import (
    check_cdn_stream_logs,
    check_content_ratings,
    check_devices_cdc,
    check_profiles,
    check_promotion_redemptions,
    check_promotions,
    check_support_tickets,
)
from src.core.transformations import clean_support_tickets

pytestmark = pytest.mark.slow

RECENT = (datetime.now(UTC) - timedelta(hours=1)).isoformat()

DEVICE_SCHEMA = [
    "event_type",
    "device_id",
    "user_id",
    "device_type",
    "os_family",
    "os_version",
    "app_version",
    "is_primary",
    "change_timestamp",
]


def _devices_row(**overrides):
    row = {
        "event_type": "insert",
        "device_id": "dev_1",
        "user_id": "user_1",
        "device_type": "tv",
        "os_family": "tizen",
        "os_version": "1.0",
        "app_version": "3.1.0",
        "is_primary": True,
        "change_timestamp": "2024-01-01T00:00:00+00:00",
    }
    row.update(overrides)
    return tuple(row[c] for c in DEVICE_SCHEMA)


# --- devices ---


def test_devices_null_device_id_quarantined(spark):
    # valid row first so the None device_id column still infers as string
    df = spark.createDataFrame(
        [_devices_row(), _devices_row(device_id=None)], schema=DEVICE_SCHEMA
    )
    result = check_devices_cdc(df)
    assert result.fail_count == 1


def test_devices_invalid_event_type_quarantined(spark):
    df = spark.createDataFrame(
        [_devices_row(event_type="upsert")], schema=DEVICE_SCHEMA
    )
    result = check_devices_cdc(df)
    assert result.fail_count == 1


def test_devices_valid_record_passes(spark):
    df = spark.createDataFrame([_devices_row()], schema=DEVICE_SCHEMA)
    result = check_devices_cdc(df)
    assert result.pass_count == 1
    assert result.fail_count == 0


# --- profiles ---


def test_profiles_invalid_language_quarantined(spark):
    df = spark.createDataFrame(
        [
            (
                "prof_1",
                "user_1",
                "Alice",
                False,
                "xx",
                "fox",
                "2024-01-01T00:00:00+00:00",
            )
        ],
        schema=[
            "profile_id",
            "user_id",
            "profile_name",
            "is_kids",
            "language",
            "avatar",
            "created_at",
        ],
    )
    result = check_profiles(df)
    assert result.fail_count == 1


def test_profiles_duplicate_profile_id_quarantined(spark):
    rows = [
        ("prof_1", "user_1", "Alice", False, "en", "fox", "2024-01-01T00:00:00+00:00")
    ] * 2
    df = spark.createDataFrame(
        rows,
        schema=[
            "profile_id",
            "user_id",
            "profile_name",
            "is_kids",
            "language",
            "avatar",
            "created_at",
        ],
    )
    result = check_profiles(df)
    assert result.fail_count == 1


# --- promotions ---

PROMO_SCHEMA = [
    "promo_code",
    "description",
    "discount_pct",
    "starts_at",
    "ends_at",
    "eligible_plan_tier",
    "max_redemptions",
    "is_active",
]
_PROMO_VALID = (
    "PROMO_OK",
    "desc",
    10,
    "2024-01-01T00:00:00+00:00",
    "2024-03-01T00:00:00+00:00",
    "basic",
    100,
    True,
)


def test_promotions_end_before_start_quarantined(spark):
    df = spark.createDataFrame(
        [
            _PROMO_VALID,
            (
                "PROMO_X",
                "desc",
                10,
                "2024-02-01T00:00:00+00:00",
                "2024-01-01T00:00:00+00:00",
                "basic",
                100,
                True,
            ),
        ],
        schema=PROMO_SCHEMA,
    )
    result = check_promotions(df)
    assert result.fail_count == 1


def test_promotions_discount_out_of_range_quarantined(spark):
    df = spark.createDataFrame(
        [
            _PROMO_VALID,
            (
                "PROMO_X",
                "desc",
                150,
                "2024-01-01T00:00:00+00:00",
                "2024-03-01T00:00:00+00:00",
                "basic",
                100,
                True,
            ),
        ],
        schema=PROMO_SCHEMA,
    )
    result = check_promotions(df)
    assert result.fail_count == 1


# --- promotion_redemptions ---


def test_redemptions_orphaned_promo_code_quarantined(spark):
    df = spark.createDataFrame(
        [
            (
                "red_1",
                "PROMO_MISSING",
                "user_1",
                "sub_1",
                "2024-01-15T00:00:00+00:00",
                "2024-01-01",
            )
        ],
        schema=[
            "redemption_id",
            "promo_code",
            "user_id",
            "subscription_id",
            "redeemed_at",
            "billing_period_start",
        ],
    )
    promos = spark.createDataFrame([("PROMO_REAL",)], schema=["promo_code"])
    result = check_promotion_redemptions(df, promo_codes=promos)
    assert result.fail_count == 1
    reasons = [
        r["quarantine_reason"]
        for r in result.quarantined.select("quarantine_reason").collect()
    ]
    assert any("orphaned" in (r or "") for r in reasons)


def test_redemptions_valid_promo_passes(spark):
    df = spark.createDataFrame(
        [
            (
                "red_1",
                "PROMO_REAL",
                "user_1",
                "sub_1",
                "2024-01-15T00:00:00+00:00",
                "2024-01-01",
            )
        ],
        schema=[
            "redemption_id",
            "promo_code",
            "user_id",
            "subscription_id",
            "redeemed_at",
            "billing_period_start",
        ],
    )
    promos = spark.createDataFrame([("PROMO_REAL",)], schema=["promo_code"])
    result = check_promotion_redemptions(df, promo_codes=promos)
    assert result.pass_count == 1
    assert result.fail_count == 0


def test_redemptions_duplicate_id_quarantined(spark):
    row = (
        "red_1",
        "PROMO_REAL",
        "user_1",
        "sub_1",
        "2024-01-15T00:00:00+00:00",
        "2024-01-01",
    )
    df = spark.createDataFrame(
        [row, row],
        schema=[
            "redemption_id",
            "promo_code",
            "user_id",
            "subscription_id",
            "redeemed_at",
            "billing_period_start",
        ],
    )
    result = check_promotion_redemptions(df)
    assert result.fail_count == 1


# --- support_tickets ---


def _ticket_row(payload: str) -> tuple:
    return (
        "tkt_1",
        "user_1",
        "2024-01-01T00:00:00+00:00",
        "2024-01-02T00:00:00+00:00",
        "resolved",
        "chat",
        5,
        payload,
    )


TICKET_SCHEMA = [
    "ticket_id",
    "user_id",
    "created_at",
    "updated_at",
    "status",
    "channel",
    "csat_score",
    "payload",
]


def test_ticket_unparseable_payload_quarantined(spark):
    valid_payload = json.dumps(
        {
            "category": "playback",
            "subcategory": "buffering",
            "device": {"device_type": "tv", "os_family": "tizen"},
            "resolution_notes": "fixed",
        }
    )
    malformed = '{"category": "playback", "subcategory"'  # truncated JSON
    df = spark.createDataFrame(
        [_ticket_row(valid_payload), _ticket_row(malformed)], schema=TICKET_SCHEMA
    )
    result = check_support_tickets(clean_support_tickets(df))
    assert result.fail_count == 1
    reasons = [
        r["quarantine_reason"]
        for r in result.quarantined.select("quarantine_reason").collect()
    ]
    assert any("unparseable" in (r or "") for r in reasons)


def test_ticket_payload_flattened_on_pass(spark):
    valid_payload = json.dumps(
        {
            "category": "playback",
            "subcategory": "buffering",
            "device": {"device_type": "tv", "os_family": "tizen"},
            "resolution_notes": "fixed",
        }
    )
    df = spark.createDataFrame([_ticket_row(valid_payload)], schema=TICKET_SCHEMA)
    result = check_support_tickets(clean_support_tickets(df))
    assert result.pass_count == 1
    passed = result.passed.collect()[0]
    assert passed["category"] == "playback"
    assert passed["payload_device_type"] == "tv"


def test_ticket_csat_out_of_range_quarantined(spark):
    valid_payload = json.dumps(
        {
            "category": "billing",
            "subcategory": "refund",
            "device": {"device_type": "web", "os_family": "windows"},
            "resolution_notes": "ok",
        }
    )
    df = spark.createDataFrame(
        [
            (
                "tkt_1",
                "user_1",
                "2024-01-01T00:00:00+00:00",
                "2024-01-02T00:00:00+00:00",
                "resolved",
                "chat",
                9,
                valid_payload,
            )
        ],
        schema=TICKET_SCHEMA,
    )
    result = check_support_tickets(clean_support_tickets(df))
    assert result.fail_count == 1


# --- cdn_stream_logs ---


def _cdn_row(**overrides):
    row = {
        "log_id": "log_1",
        "session_id": "s1",
        "user_id": "user_1",
        "content_id": "ct_1",
        "device_type": "tv",
        "event_timestamp": RECENT,
        "bitrate_kbps": 5000,
        "rebuffer_ms": 100,
        "startup_ms": 800,
    }
    row.update(overrides)
    return tuple(
        row[c]
        for c in [
            "log_id",
            "session_id",
            "user_id",
            "content_id",
            "device_type",
            "event_timestamp",
            "bitrate_kbps",
            "rebuffer_ms",
            "startup_ms",
        ]
    )


CDN_SCHEMA = [
    "log_id",
    "session_id",
    "user_id",
    "content_id",
    "device_type",
    "event_timestamp",
    "bitrate_kbps",
    "rebuffer_ms",
    "startup_ms",
]


def test_cdn_null_bitrate_quarantined(spark):
    df = spark.createDataFrame(
        [_cdn_row(), _cdn_row(log_id="log_2", bitrate_kbps=None)], schema=CDN_SCHEMA
    )
    result = check_cdn_stream_logs(df)
    assert result.fail_count == 1


def test_cdn_duplicate_log_id_quarantined(spark):
    df = spark.createDataFrame([_cdn_row(), _cdn_row()], schema=CDN_SCHEMA)
    result = check_cdn_stream_logs(df)
    assert result.fail_count == 1


def test_cdn_negative_rebuffer_quarantined(spark):
    df = spark.createDataFrame(
        [_cdn_row(), _cdn_row(log_id="log_2", rebuffer_ms=-100)], schema=CDN_SCHEMA
    )
    result = check_cdn_stream_logs(df)
    assert result.fail_count == 1


def test_cdn_valid_record_passes(spark):
    df = spark.createDataFrame([_cdn_row()], schema=CDN_SCHEMA)
    result = check_cdn_stream_logs(df)
    assert result.pass_count == 1
    assert result.fail_count == 0


# --- content_ratings ---


def _rating_row(**overrides):
    row = {
        "rating_id": "rt_1",
        "user_id": "user_1",
        "content_id": "ct_1",
        "rating": 4,
        "review_text": "great",
        "rated_at": "2024-01-01T00:00:00+00:00",
        "updated_at": "2024-01-01T00:00:00+00:00",
    }
    row.update(overrides)
    return tuple(
        row[c]
        for c in [
            "rating_id",
            "user_id",
            "content_id",
            "rating",
            "review_text",
            "rated_at",
            "updated_at",
        ]
    )


RATING_SCHEMA = [
    "rating_id",
    "user_id",
    "content_id",
    "rating",
    "review_text",
    "rated_at",
    "updated_at",
]


def test_rating_out_of_range_quarantined(spark):
    df = spark.createDataFrame(
        [_rating_row(), _rating_row(rating_id="rt_2", rating=6)], schema=RATING_SCHEMA
    )
    result = check_content_ratings(df)
    assert result.fail_count == 1


def test_rating_null_user_quarantined(spark):
    df = spark.createDataFrame(
        [_rating_row(), _rating_row(rating_id="rt_2", user_id=None)],
        schema=RATING_SCHEMA,
    )
    result = check_content_ratings(df)
    assert result.fail_count == 1


def test_rating_valid_record_passes(spark):
    df = spark.createDataFrame([_rating_row()], schema=RATING_SCHEMA)
    result = check_content_ratings(df)
    assert result.pass_count == 1
    assert result.fail_count == 0
