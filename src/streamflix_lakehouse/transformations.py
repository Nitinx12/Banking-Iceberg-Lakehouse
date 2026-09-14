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


def dedupe_on_key(df: DataFrame, keys: list[str], order_by: str | None = None) -> DataFrame:
    """Deduplicate on natural keys, keeping latest by order_by if provided."""
    if order_by and order_by in df.columns:
        w = F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"]).Window.partitionBy(keys).orderBy(
                F.col(order_by).desc()
            )
        )
        return df.withColumn("_rn", w).filter(F.col("_rn") == 1).drop("_rn")
    return df.dropDuplicates(keys)


def with_ingestion_metadata(df: DataFrame, source_file_col: str = "_source_file") -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name() if source_file_col else F.lit(None))
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
            F.when(F.col("watch_duration_seconds") < 0, None).otherwise(F.col("watch_duration_seconds")),
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
