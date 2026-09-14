"""SCD Type 2 merge logic — generic (any CDC dimension) + subscriptions wrappers.

Source spec: CDC append-only with event_type (insert/update/delete), a natural
key, tracked attribute columns, and a change timestamp.

Target schema (silver.<dim>_scd2):
  <key>, <tracked cols...>, effective_date (timestamp), end_date (timestamp,
  nullable), is_current (boolean).

Idempotency: re-processing the same CDC batch must not duplicate history rows.
Strategy: staged events are deduplicated by (key, change_timestamp) and
skipped when (key, effective_date) already exists in the target.
For environments without Delta MERGE available locally, we expose a DataFrame-only
apply_scd2_generic() that tests can call; production notebooks use MERGE.

`apply_scd2` / `build_merge_sql` keep the original subscriptions-specific
signatures as thin wrappers so existing callers and tests are unaffected.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _parse_and_sort_cdc(cdc_df: DataFrame, key_col: str, ts_col: str) -> DataFrame:
    """Ensure the change timestamp is a timestamp type, dedupe, and sort per key."""
    if ts_col in cdc_df.columns:
        # handle both string and timestamp inputs; keep original if unparseable
        cdc_df = cdc_df.withColumn(
            ts_col,
            F.coalesce(F.to_timestamp(F.col(ts_col)), F.col(ts_col)),
        )
    # dedupe exact CDC duplicates (idempotency pre-step)
    cdc_df = cdc_df.dropDuplicates([key_col, ts_col])
    return cdc_df.orderBy(key_col, ts_col)


def apply_scd2_generic(
    target_df: DataFrame | None,
    cdc_df: DataFrame,
    key_col: str,
    tracked_cols: list[str],
    ts_col: str = "change_timestamp",
    event_col: str = "event_type",
) -> DataFrame:
    """Pure DataFrame SCD2 implementation for any CDC dimension (local tests).

    - Closes current row (is_current=true) by setting end_date = new change_timestamp
    - Inserts new row with effective_date = change_timestamp, is_current = (event != delete)
    - delete events close the current row and do NOT open a new current row
    - Re-running same batch is idempotent: if (key, effective_date) already exists, skip
    """
    cdc = _parse_and_sort_cdc(cdc_df, key_col, ts_col)
    out_cols = [key_col, *tracked_cols, "effective_date", "end_date", "is_current"]

    if target_df is None or target_df.rdd.isEmpty():
        # Bootstrap: each CDC event becomes a row; window to set end_date
        w = Window.partitionBy(key_col).orderBy(ts_col)
        cdc = cdc.withColumn("end_date", F.lead(ts_col).over(w))
        # is_current = last row per key AND not deleted
        w_desc = Window.partitionBy(key_col).orderBy(F.col(ts_col).desc())
        cdc = cdc.withColumn("_rn", F.row_number().over(w_desc))
        result = (
            cdc.withColumn("effective_date", F.col(ts_col))
            .withColumn(
                "is_current",
                F.when(
                    (F.col("_rn") == 1) & (F.col(event_col) != "delete"),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
            .withColumn(
                "end_date",
                F.when(F.col("is_current"), None).otherwise(F.col("end_date")),
            )
            .select(*out_cols)
        )
        # A trailing delete stays in history as a closed row: is_current false,
        # end_date null (closed without successor) — kept for lineage/audit.
        return result

    # Incremental: iterate CDC events in order and apply row-wise
    # For test-scale data this is fine; production uses Delta MERGE.
    # To keep it DataFrame-native we collect CDC to driver (bounded test sizes).

    cdc_rows = cdc.orderBy(ts_col).collect()
    # Build idempotency key set from target
    existing_keys = set(
        target_df.select(key_col, "effective_date")
        .rdd.map(lambda r: (r[0], str(r[1])))
        .collect()
    )

    result_df = target_df

    for row in cdc_rows:
        r = row.asDict()
        eff = r[ts_col]
        key = (r[key_col], str(eff))
        if key in existing_keys:
            continue  # idempotent skip

        # close current row for this key if exists
        result_df = result_df.withColumn(
            "end_date",
            F.when(
                (F.col(key_col) == r[key_col]) & (F.col("is_current") == True),
                F.lit(eff),
            ).otherwise(F.col("end_date")),
        ).withColumn(
            "is_current",
            F.when(
                (F.col(key_col) == r[key_col]) & (F.col("is_current") == True),
                F.lit(False),
            ).otherwise(F.col("is_current")),
        )

        if r[event_col] == "delete":
            existing_keys.add(key)
            continue

        # insert new current row
        spark = result_df.sparkSession
        new_row = spark.createDataFrame(
            [[r[key_col], *[r[c] for c in tracked_cols], eff, None, True]],
            schema=out_cols,
        )
        result_df = result_df.unionByName(new_row, allowMissingColumns=True)
        existing_keys.add(key)

    return result_df


def build_merge_sql_generic(
    target_table: str,
    cdc_view: str,
    key_col: str,
    tracked_cols: list[str],
    ts_col: str = "change_timestamp",
    event_col: str = "event_type",
) -> str:
    """Return the Delta MERGE SQL for any CDC dimension (production notebooks).

    Notebooks should create a deduped temp view of the CDC source, then run this.
    """
    insert_cols = [key_col, *tracked_cols, "effective_date", "end_date", "is_current"]
    insert_vals = [
        f"src.{key_col}",
        *[f"src.{c}" for c in tracked_cols],
        f"src.{ts_col}",
        "NULL",
        "true",
    ]
    return f"""
MERGE INTO {target_table} AS tgt
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_col}, {ts_col} ORDER BY {ts_col}) AS rn
    FROM {cdc_view}
  ) WHERE rn = 1
) AS src
ON tgt.{key_col} = src.{key_col} AND tgt.is_current = true
WHEN MATCHED AND src.{event_col} IN ('update', 'delete') THEN
  UPDATE SET tgt.end_date = src.{ts_col}, tgt.is_current = false
WHEN NOT MATCHED AND src.{event_col} IN ('insert', 'update') THEN
  INSERT ({", ".join(insert_cols)})
  VALUES ({", ".join(insert_vals)})
"""


# --- subscriptions wrappers (original API, kept for backward compatibility) ---

SUBSCRIPTIONS_TRACKED_COLS = ["user_id", "plan_tier", "status"]


def apply_scd2(target_df: DataFrame | None, cdc_df: DataFrame) -> DataFrame:
    """SCD2 for the subscriptions dimension — see apply_scd2_generic."""
    return apply_scd2_generic(
        target_df,
        cdc_df,
        key_col="subscription_id",
        tracked_cols=SUBSCRIPTIONS_TRACKED_COLS,
    )


def build_merge_sql(target_table: str, cdc_view: str) -> str:
    """Delta MERGE SQL for the subscriptions dimension — see build_merge_sql_generic."""
    return build_merge_sql_generic(
        target_table,
        cdc_view,
        key_col="subscription_id",
        tracked_cols=SUBSCRIPTIONS_TRACKED_COLS,
    )
