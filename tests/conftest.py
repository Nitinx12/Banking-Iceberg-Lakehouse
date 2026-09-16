import os
import sys

# Ensure Spark workers use the venv python on Windows
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

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
    # Billing idempotency uses its own Delta session (see test_billing_idempotency.py).
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
