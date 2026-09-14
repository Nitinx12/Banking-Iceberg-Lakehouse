"""Reusable PySpark transformations — unit-testable, no cluster required."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def normalize_strings(df: DataFrame, cols: list[str]) -> DataFrame:
    """Trim and lower/strip where appropriate."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


def standardize_timestamps(df: DataFrame, cols: list[str]) -> DataFrame:
    """Cast ISO strings to timestamp; assume UTC."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, F.to_timestamp(F.col(c)))
    return df


def dedupe_on_key(
    df: DataFrame, keys: list[str], order_by: str | None = None
) -> DataFrame:
    """Deduplicate on natural keys, keeping latest by order_by if provided."""
    if order_by and order_by in df.columns:
        w = F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy(keys)
            .orderBy(F.col(order_by).desc())
        )
        return df.withColumn("_rn", w).filter(F.col("_rn") == 1).drop("_rn")
    return df.dropDuplicates(keys)


def with_ingestion_metadata(
    df: DataFrame, source_file_col: str = "_source_file"
) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn(
            "_source_file", F.input_file_name() if source_file_col else F.lit(None)
        )
        .withColumn("_batch_id", F.lit(None).cast("string"))
    )


def clean_watch_events(df: DataFrame) -> DataFrame:
    """Silver cleaning for watch_events."""
    df = normalize_strings(df, ["event_type", "device_type"])
    df = standardize_timestamps(df, ["event_timestamp"])
    # normalize durations: negative -> null, null -> 0 handled downstream by quality gate
    if "watch_duration_seconds" in df.columns:
        df = df.withColumn(
            "watch_duration_seconds",
            F.when(F.col("watch_duration_seconds") < 0, None).otherwise(
                F.col("watch_duration_seconds")
            ),
        )
    df = dedupe_on_key(df, ["event_id"], order_by="event_timestamp")
    return df


def clean_billing(df: DataFrame) -> DataFrame:
    df = standardize_timestamps(df, ["transaction_timestamp"])
    df = normalize_strings(df, ["currency", "transaction_type", "plan_tier"])
    df = dedupe_on_key(df, ["transaction_id"], order_by="transaction_timestamp")
    return df


def clean_content_catalog(df: DataFrame) -> DataFrame:
    df = normalize_strings(df, ["genre", "content_type", "rating", "title"])
    df = standardize_timestamps(df, ["release_date", "added_at"])
    df = dedupe_on_key(df, ["content_id"])
    return df


# --- new tables ---


def clean_devices_cdc(df: DataFrame) -> DataFrame:
    """Silver cleaning for devices CDC. No dedupe here — SCD2 dedupes on (device_id, change_timestamp)."""
    df = normalize_strings(df, ["device_type", "os_family"])
    df = standardize_timestamps(df, ["change_timestamp"])
    return df


def clean_profiles(df: DataFrame) -> DataFrame:
    df = normalize_strings(df, ["profile_name", "language", "avatar"])
    df = standardize_timestamps(df, ["created_at"])
    df = dedupe_on_key(df, ["profile_id"])
    return df


def clean_promotions(df: DataFrame) -> DataFrame:
    df = normalize_strings(df, ["promo_code", "eligible_plan_tier"])
    df = standardize_timestamps(df, ["starts_at", "ends_at"])
    df = dedupe_on_key(df, ["promo_code"])
    return df


def clean_promotion_redemptions(df: DataFrame) -> DataFrame:
    df = normalize_strings(df, ["promo_code"])
    df = standardize_timestamps(df, ["redeemed_at", "billing_period_start"])
    df = dedupe_on_key(df, ["redemption_id"], order_by="redeemed_at")
    return df


def clean_support_tickets(df: DataFrame) -> DataFrame:
    """Silver cleaning + semi-structured payload parsing for support_tickets.

    The nested JSON `payload` string is parsed with an explicit schema and
    flattened into top-level columns. Malformed payloads parse to nulls and
    are quarantined by check_support_tickets (unparseable_payload), not dropped.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    df = standardize_timestamps(df, ["created_at", "updated_at"])
    df = normalize_strings(df, ["status", "channel"])
    payload_schema = StructType(
        [
            StructField("category", StringType(), True),
            StructField("subcategory", StringType(), True),
            StructField(
                "device",
                StructType(
                    [
                        StructField("device_type", StringType(), True),
                        StructField("os_family", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("resolution_notes", StringType(), True),
        ]
    )
    parsed = F.from_json(F.col("payload"), payload_schema)
    return (
        df.withColumn("_payload", parsed)
        .withColumn("category", F.col("_payload.category"))
        .withColumn("subcategory", F.col("_payload.subcategory"))
        .withColumn("payload_device_type", F.col("_payload.device.device_type"))
        .withColumn("payload_os_family", F.col("_payload.device.os_family"))
        .withColumn("resolution_notes", F.col("_payload.resolution_notes"))
        .drop("_payload")
    )


def clean_cdn_stream_logs(df: DataFrame) -> DataFrame:
    df = normalize_strings(df, ["device_type", "cdn_edge"])
    df = standardize_timestamps(df, ["event_timestamp"])
    df = dedupe_on_key(df, ["log_id"], order_by="event_timestamp")
    return df


def clean_content_ratings(df: DataFrame) -> DataFrame:
    """Silver cleaning for content_ratings. No dedupe here — the quality gate
    runs first, then latest-wins dedupe on (user_id, content_id) is applied
    post-gate so bad re-ratings get quarantined instead of silently dropped."""
    df = standardize_timestamps(df, ["rated_at", "updated_at"])
    return df


def latest_rating_per_user_content(df: DataFrame) -> DataFrame:
    """Upsert semantics: keep only the latest rating per (user_id, content_id).

    Distinct from PK dedupe — re-ratings carry a new rating_id, so the natural
    key of this table is (user_id, content_id), not rating_id.
    """
    return dedupe_on_key(df, ["user_id", "content_id"], order_by="updated_at")
