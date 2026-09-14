"""SCD Type 2 merge logic for subscriptions — unit-testable pure DataFrame functions.

Source spec: CDC append-only with event_type (insert/update/delete),
subscription_id, user_id, plan_tier, status, change_timestamp, previous_plan_tier.

Target schema (silver.subscriptions_scd2):
  subscription_id, user_id, plan_tier, status,
  effective_date (timestamp), end_date (timestamp, nullable),
  is_current (boolean), hash or previous_plan_tier optional.

Idempotency: re-processing the same CDC batch must not duplicate history rows.
Strategy: staged events are deduplicated by (subscription_id, change_timestamp) and
joined against target on subscription_id + effective_date before insert.
For environments without Delta MERGE available locally, we expose a DataFrame-only
apply_scd2() that tests can call; production notebooks use MERGE via io_utils.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _parse_and_sort_cdc(cdc_df: DataFrame) -> DataFrame:
    """Ensure change_timestamp is timestamp and sort per key."""
    if "change_timestamp" in cdc_df.columns:
        # handle both string and timestamp inputs
        cdc_df = cdc_df.withColumn(
            "change_timestamp",
            F.when(
                F.col("change_timestamp").cast("string") == F.col("change_timestamp").cast("string"),
                F.to_timestamp(F.col("change_timestamp")),
            ).otherwise(F.col("change_timestamp")),
        )
        # fallback if above produced nulls for already-timestamp cols
        cdc_df = cdc_df.withColumn(
            "change_timestamp",
            F.coalesce(F.to_timestamp(F.col("change_timestamp")), F.col("change_timestamp")),
        )
    # dedupe exact CDC duplicates (idempotency pre-step)
    cdc_df = cdc_df.dropDuplicates(["subscription_id", "change_timestamp"])
    return cdc_df.orderBy("subscription_id", "change_timestamp")


def apply_scd2(
    target_df: DataFrame | None,
    cdc_df: DataFrame,
) -> DataFrame:
    """Pure DataFrame SCD2 implementation for local tests.

    - Closes current row (is_current=true) by setting end_date = new change_timestamp
    - Inserts new row with effective_date = change_timestamp, is_current = (event_type != delete)
    - delete events close the current row and do NOT open a new current row
    - Re-running same batch is idempotent: if effective_date already exists, skip
    """
    cdc = _parse_and_sort_cdc(cdc_df)

    if target_df is None or target_df.rdd.isEmpty():
        # Bootstrap: each CDC event becomes a row; window to set end_date
        w = Window.partitionBy("subscription_id").orderBy("change_timestamp")
        cdc = cdc.withColumn("end_date", F.lead("change_timestamp").over(w))
        # is_current = last row per key AND not deleted
        w_desc = Window.partitionBy("subscription_id").orderBy(F.col("change_timestamp").desc())
        cdc = cdc.withColumn("_rn", F.row_number().over(w_desc))
        result = (
            cdc.withColumn("effective_date", F.col("change_timestamp"))
            .withColumn(
                "is_current",
                F.when(
                    (F.col("_rn") == 1) & (F.col("event_type") != "delete"),
                    F.lit(True),
                ).otherwise(F.lit(False)),
            )
            .withColumn(
                "end_date",
                F.when(F.col("is_current"), None).otherwise(F.col("end_date")),
            )
            .drop("_rn", "change_timestamp", "event_type", "previous_plan_tier")
        )
        # for delete-last case, the final row should still be non-current with end_date null?
        # Correct: last delete closes previous, no new current -> drop the delete's own row if you'd prefer.
        # We keep delete row as closed history for audit but mark not current; alternative is to exclude it.
        # Here we keep it for lineage: delete -> is_current false, end_date null (closed without successor).
        result = result.withColumn(
            "end_date",
            F.when((F.col("is_current") == False) & F.col("end_date").isNull(), None).otherwise(
                F.col("end_date")
            ),
        )
        return result

    # Incremental: iterate CDC events in order and apply row-wise
    # For test-scale data this is fine; production uses Delta MERGE.
    # To keep it DataFrame-native we collect CDC to driver (bounded test sizes)
    # and use Spark operations per batch — still proven idempotent.

    # First, collect distinct subscription_ids touched
    cdc_rows = cdc.orderBy("change_timestamp").collect()
    # Build idempotency key set from target
    existing_keys = set(
        target_df.select("subscription_id", "effective_date")
        .rdd.map(lambda r: (r[0], str(r[1])))
        .collect()
    )


    # We'll iteratively build result DataFrame
    result_df = target_df

    for row in cdc_rows:
        r = row.asDict()
        eff = r["change_timestamp"]
        key = (r["subscription_id"], str(eff))
        if key in existing_keys:
            continue  # idempotent skip

        # close current row for this subscription_id if exists
        result_df = result_df.withColumn(
            "end_date",
            F.when(
                (F.col("subscription_id") == r["subscription_id"]) & (F.col("is_current") == True),
                F.lit(eff),
            ).otherwise(F.col("end_date")),
        ).withColumn(
            "is_current",
            F.when(
                (F.col("subscription_id") == r["subscription_id"]) & (F.col("is_current") == True),
                F.lit(False),
            ).otherwise(F.col("is_current")),
        )

        if r["event_type"] == "delete":
            existing_keys.add(key)
            continue

        # insert new current row
        spark = result_df.sparkSession
        new_row = spark.createDataFrame(
            [
                (
                    r["subscription_id"],
                    r["user_id"],
                    r["plan_tier"],
                    r["status"],
                    eff,
                    None,
                    True,
                )
            ],
            schema=["subscription_id", "user_id", "plan_tier", "status", "effective_date", "end_date", "is_current"],
        )
        # align columns
        result_df = result_df.unionByName(new_row, allowMissingColumns=True)
        existing_keys.add(key)

    return result_df


def build_merge_sql(target_table: str, cdc_view: str) -> str:
    """Return the Delta MERGE SQL for production notebooks.

    Notebooks should create a temp view `cdc_deduped` sorted/deduped, then run this.
    """
    return f"""
MERGE INTO {target_table} AS tgt
USING (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY subscription_id, change_timestamp ORDER BY change_timestamp) AS rn
    FROM {cdc_view}
  ) WHERE rn = 1
) AS src
ON tgt.subscription_id = src.subscription_id AND tgt.is_current = true
WHEN MATCHED AND src.event_type IN ('update', 'delete') THEN
  UPDATE SET tgt.end_date = src.change_timestamp, tgt.is_current = false
WHEN NOT MATCHED AND src.event_type IN ('insert', 'update') THEN
  INSERT (subscription_id, user_id, plan_tier, status, effective_date, end_date, is_current)
  VALUES (src.subscription_id, src.user_id, src.plan_tier, src.status, src.change_timestamp, NULL, true)
"""
