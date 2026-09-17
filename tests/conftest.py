import os
import sys

# Ensure Spark workers use the venv python on Windows
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    # Fast non-Delta session for most tests (7s startup vs 19s Delta).
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("streamflix-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def spark_delta():
    # Delta-enabled session for tests that write Delta tables (slower startup ~19s).
    # Creates session directly with configure_spark_with_delta_pip to ensure Delta jars are loaded.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    from pyspark.sql import SparkSession
    # Stop any existing active session to ensure clean Delta config
    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()

    from src.config import get_config
    from delta import configure_spark_with_delta_pip

    cfg = get_config()
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("streamflix-tests-delta")
        .config("spark.sql.shuffle.partitions", str(cfg.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", cfg.warehouse_dir)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
