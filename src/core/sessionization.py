"""Gap-based sessionization — pure, unit-testable DataFrame functions.

Splits an ordered event stream into sessions: a new session starts whenever
the gap since the previous event (per key) exceeds `gap_minutes` of inactivity.
Used by Silver cdn_stream_logs to roll QoE telemetry up to per-session metrics.

Interview talking point: this is the batch equivalent of Spark Structured
Streaming's session window (`session_window` with gap duration); a streaming
upgrade would replace the lag/running-sum window with
`groupBy(session_window(event_timestamp, "30 minutes"))`.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def sessionize(
    df: DataFrame,
    key: str = "session_id",
    ts_col: str = "event_timestamp",
    gap_minutes: int = 30,
    session_col: str = "session_number",
) -> DataFrame:
    """Add a per-key session number: increments when inactivity gap > gap_minutes.

    - Events within `gap_minutes` of the previous event belong to the same session
      (a gap of exactly `gap_minutes` is still the same session).
    - The first event of each key starts session 1.
    - Timestamps are cast to timestamp type; per-key ordering handles
      out-of-order input because the window sorts before differencing.
    """
    df = df.withColumn(ts_col, F.to_timestamp(F.col(ts_col)))
    # pre-partition by key to chunk the window — 2 partitions is enough for
    # test-scale data (was 8, caused oversharding on tiny DFs; prod large
    # datasets are handled via cfg.spark_shuffle_partitions on write)
    df = df.repartition(2, key)

    w_ordered = Window.partitionBy(key).orderBy(ts_col)
    gap_seconds = F.unix_timestamp(F.col(ts_col)) - F.lag(
        F.unix_timestamp(F.col(ts_col))
    ).over(w_ordered)
    # first event per key (null gap) starts a session; so does any gap > threshold
    starts_session = F.coalesce((gap_seconds > gap_minutes * 60).cast("int"), F.lit(1))

    w_running = (
        Window.partitionBy(key)
        .orderBy(ts_col)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return df.withColumn(session_col, F.sum(starts_session).over(w_running))


def rollup_sessions(
    sessionized: DataFrame,
    key: str = "session_id",
    ts_col: str = "event_timestamp",
    session_col: str = "session_number",
) -> DataFrame:
    """Collapse sessionized events to one row per (key, session_number).

    QoE metrics: session start/end, log count, total rebuffer, average bitrate,
    and startup latency (first log of the session).
    """
    w_first = Window.partitionBy(key, session_col).orderBy(ts_col)
    return (
        sessionized.withColumn("_startup_ms", F.first("startup_ms").over(w_first))
        .groupBy(key, session_col)
        .agg(
            F.min(ts_col).alias("session_start"),
            F.max(ts_col).alias("session_end"),
            F.first("user_id").alias("user_id"),
            F.first("content_id").alias("content_id"),
            F.first("device_type").alias("device_type"),
            F.count(F.lit(1)).alias("n_logs"),
            F.sum(F.coalesce(F.col("rebuffer_ms"), F.lit(0))).alias(
                "total_rebuffer_ms"
            ),
            F.avg(F.col("bitrate_kbps")).alias("avg_bitrate_kbps"),
            F.first("_startup_ms").alias("startup_ms"),
        )
        .withColumn("session_date", F.to_date(F.col("session_start")))
    )
