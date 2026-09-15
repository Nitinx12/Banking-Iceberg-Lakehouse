"""IO helpers: Delta writes, Hive DB routing, idempotent helpers."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def ensure_db(spark, db: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")


def write_delta(
    df: DataFrame,
    table: str,
    mode: str = "append",
    partition_by: list[str] | None = None,
    merge_schema: bool = True,
) -> None:
    writer = df.write.format("delta").mode(mode)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    try:
        writer.saveAsTable(table)
    except Exception as e:
        if "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION" in str(e):
            # metastore lost but location exists — append via path then re-register
            try:
                spark = df.sparkSession
                db, tbl = table.split(".") if "." in table else ("default", table)
                # try to infer warehouse location
                try:
                    loc = spark.sql(f"DESCRIBE DETAIL {table}").select("location").first()[0]  # type: ignore
                except Exception:
                    from src.config import get_config

                    cfg = get_config()
                    base = cfg.warehouse_dir or ".spark/warehouse"
                    loc = f"{base}/{db}.db/{tbl}"
                # re-normalize to file URI
                if not loc.startswith("file:"):
                    import pathlib

                    loc = pathlib.Path(loc).as_posix()
                    if not loc.startswith("/"):
                        loc = f"file:/{loc}" if loc[1] == ":" else f"file://{loc}"
                df.write.format("delta").mode(mode).option("mergeSchema", "true").save(loc.replace("file:", ""))
                try:
                    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} USING DELTA LOCATION '{loc}'")
                except Exception:
                    pass
                return
            except Exception:
                pass
        raise


def write_quarantine(df: DataFrame, table: str) -> None:
    """Append quarantined rows partitioned by date."""
    if "event_timestamp" in df.columns:
        df = df.withColumn("_q_date", F.to_date(F.col("event_timestamp")))
        partition = ["_q_date"]
    elif "change_timestamp" in df.columns:
        df = df.withColumn("_q_date", F.to_date(F.col("change_timestamp")))
        partition = ["_q_date"]
    else:
        df = df.withColumn("_q_date", F.current_date())
        partition = ["_q_date"]
    write_delta(df, table, mode="append", partition_by=partition)


def log_audit(
    spark, audit_table: str, layer: str, table: str, passed: int, failed: int
) -> None:
    ensure_db(spark, audit_table.split(".")[0] if "." in audit_table else "silver")
    import datetime

    # python datetime, not F.current_timestamp() — Column objects can't be
    # schema-inferred by createDataFrame
    audit_df = spark.createDataFrame(
        [(layer, table, passed, failed, datetime.datetime.now(datetime.UTC))],
        schema=["layer", "table_name", "pass_count", "fail_count", "run_at"],
    )
    write_delta(audit_df, audit_table, mode="append")
