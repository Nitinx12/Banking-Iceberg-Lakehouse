"""Spark engine + FQN helpers — respects SPARK_* / catalog vars in .env."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def get_spark(app_name: str = "streamflix") -> SparkSession:
    """Create or get SparkSession tuned from src/config.py.

    On Databricks, `spark` already exists — this just applies config.
    Locally, it builds a local[2] session (see tests/conftest.py pattern).
    """
    from pyspark.sql import SparkSession

    from src.config import get_config

    cfg = get_config()
    builder = SparkSession.builder.appName(app_name)
    # Local fallback if no active session
    try:
        spark = SparkSession.getActiveSession()
        if spark is not None:
            return spark
    except Exception:
        pass
    builder = builder.master("local[2]")
    spark = (
        builder.config("spark.sql.shuffle.partitions", str(cfg.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", str(cfg.spark_adaptive_enabled).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.databricks.delta.autoOptimize.optimizeWrite", "true")
        .getOrCreate()
    )
    return spark


def table_fqn(schema: str, table: str) -> str:
    """Fully-qualified UC name: `catalog`.`schema`.`table`."""
    from src.config import get_config

    cfg = get_config()
    return f"`{cfg.catalog_name}`.{schema}.{table}"


def audit_table_fqn() -> str:
    from src.config import get_config

    return get_config().audit_table


def lineage_table_fqn() -> str:
    from src.config import get_config

    return get_config().lineage_table
