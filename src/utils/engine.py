"""Spark engine + FQN helpers — respects SPARK_* / catalog vars in .env."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark(app_name: str = "streamflix") -> SparkSession:
    """Create or get SparkSession tuned from src/config.py.

    On Databricks, `spark` already exists — this just applies config.
    Locally, it builds a local[2] session with Delta Lake on the session
    catalog (warehouse under .spark/), a quiet log4j level, and the JVM
    startup banners captured so the terminal stays clean.
    """
    from pyspark.sql import SparkSession

    from src.config import get_config
    from src.utils.console import (
        mute_fds_at_exit,
        quiet_fds,
        setup_clean_output,
        show_captured,
    )

    cfg = get_config()
    # Local fallback if no active session
    try:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            return spark
    except Exception:
        pass

    builder = SparkSession.builder.appName(app_name)
    if cfg.is_local:
        setup_clean_output()
        from delta import configure_spark_with_delta_pip

        builder = (
            builder.master("local[2]")
            .config("spark.sql.shuffle.partitions", str(cfg.spark_shuffle_partitions))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", cfg.warehouse_dir)
            .config("spark.ui.enabled", "false")
        )
        # JVM banners (log4j defaults, jdk.incubator.vector) go straight to the
        # fds — capture them, and only surface them if startup actually fails.
        with quiet_fds() as noise:
            try:
                spark = configure_spark_with_delta_pip(builder).getOrCreate()
            except Exception:
                show_captured(noise)
                raise
        spark.sparkContext.setLogLevel("ERROR")
        # registered after the session exists, so it runs before pyspark's own
        # atexit handlers (LIFO) and mutes the Windows taskkill chatter
        mute_fds_at_exit()
        return spark

    spark = (
        builder.config(
            "spark.sql.shuffle.partitions", str(cfg.spark_shuffle_partitions)
        )
        .config("spark.sql.adaptive.enabled", str(cfg.spark_adaptive_enabled).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
        .getOrCreate()
    )
    return spark


def table_fqn(schema: str, table: str) -> str:
    """Fully-qualified table name: UC three-part on Databricks, two-part locally."""
    from src.config import get_config

    cfg = get_config()
    return cfg.fqn(schema, table)


def ensure_schema(spark: SparkSession, schema: str) -> None:
    """CREATE SCHEMA (UC catalog) with a Hive DATABASE fallback when UC is absent."""
    from src.config import get_config

    cfg = get_config()
    if cfg.catalog_name:
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg.catalog_name}`.{schema}")
            return
        except Exception:
            pass
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {schema}")


def ensure_schemas(spark: SparkSession) -> None:
    """Create bronze/silver/gold/init_schema up front so first runs work everywhere."""
    from src.config import get_config

    cfg = get_config()
    for schema in (
        cfg.bronze_schema,
        cfg.silver_schema,
        cfg.gold_schema,
        cfg.init_schema,
    ):
        ensure_schema(spark, schema)


def audit_table_fqn() -> str:
    from src.config import get_config

    return get_config().audit_table


def lineage_table_fqn() -> str:
    from src.config import get_config

    return get_config().lineage_table
