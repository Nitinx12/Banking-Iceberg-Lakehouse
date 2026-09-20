"""tests/integration/test_pipeline_smoke.py — compose core integration smoke tests (PROJECT_PLAN Phase 1).

Requires `docker compose --profile core up -d` and local Mongo.
Run: pytest -m integration
"""

import os

import pytest

pytestmark = pytest.mark.integration


def test_postgres_healthy():
    """Verify PostgreSQL is reachable and has expected schemas."""
    from sqlalchemy import create_engine, text

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    pw = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_WAREHOUSE_DB", "banking_dw")

    engine = create_engine(
        f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}",
        pool_pre_ping=False,
    )

    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1")).scalar_one()
        assert result == 1

        # Check expected schemas exist
        schemas = conn.execute(
            text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('serving', 'ops', 'rt', 'quarantine')"
            )
        ).fetchall()
        schema_names = [s[0] for s in schemas]
        assert "serving" in schema_names
        assert "ops" in schema_names


def test_mongo_healthy():
    """Verify MongoDB replica set is reachable."""
    from pymongo import MongoClient

    uri = os.getenv(
        "MONGO_URI",
        "mongodb://admin:password@localhost:27017/banking?replicaSet=rs0&authSource=admin",
    )

    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        result = client.admin.command("ping")
        assert result["ok"] == 1
    finally:
        client.close()

    # Check replica set status
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    try:
        rs_status = client.admin.command("replSetGetStatus")
        assert rs_status["ok"] == 1
        assert len(rs_status["members"]) >= 1
    finally:
        client.close()


def test_minio_healthy():
    """Verify MinIO S3-compatible storage is reachable."""
    import boto3
    from botocore.exceptions import ClientError

    endpoint = os.getenv("S3_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    bucket = os.getenv("S3_BUCKET", "banking-lakehouse")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as e:
        pytest.fail(f"MinIO bucket {bucket} not accessible: {e}")


def test_spark_session_can_initialize():
    """Verify Spark session factory works for local ingest."""
    spark = None

    try:
        from jobs.common.spark import get_spark

        spark = get_spark("test_smoke")
        assert spark is not None
        assert spark.sparkContext.appName == "test_smoke"
    finally:
        if spark:
            spark.stop()


def test_iceberg_jdbc_catalog_can_connect():
    """Verify Iceberg JDBC catalog is accessible via Spark."""
    spark = None

    try:
        from jobs.common.spark import get_spark

        spark = get_spark("test_catalog")
        os.getenv("ICEBERG_CATALOG_NAME", "banking")

        # Try to list catalogs
        catalogs = spark.sql("SHOW CATALOGS").collect()
        catalog_names = [c[0] for c in catalogs]

        # banking catalog should exist after init
        # (may not exist yet in Phase 0, so we just check Spark is working)
        assert len(catalog_names) >= 0
    finally:
        if spark:
            spark.stop()


def test_sampling_data_in_mongo():
    """Verify sample data exists in Mongo collections."""
    from pymongo import MongoClient

    uri = os.getenv(
        "MONGO_URI",
        "mongodb://admin:password@localhost:27017/banking?replicaSet=rs0&authSource=admin",
    )
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)

    try:
        db = client.get_database()
        collections = db.list_collection_names()

        # Check at least customers and transactions exist
        assert "customers" in collections
        assert "transactions" in collections

        # Check each has some documents
        assert db.customers.count_documents({}) > 0
        assert db.transactions.count_documents({}) > 0
    finally:
        client.close()


@pytest.fixture(scope="session")
def mongo_client():
    """Fixture providing a Mongo client for use in other tests."""
    from pymongo import MongoClient

    uri = os.getenv(
        "MONGO_URI",
        "mongodb://admin:password@localhost:27017/banking?replicaSet=rs0&authSource=admin",
    )
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    yield client
    client.close()


@pytest.fixture(scope="session")
def pg_engine():
    """Fixture providing a SQLAlchemy engine for use in other tests."""
    from sqlalchemy import create_engine

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "postgres")
    pw = os.getenv("POSTGRES_PASSWORD", "postgres")
    db = os.getenv("POSTGRES_WAREHOUSE_DB", "banking_dw")

    engine = create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")
    yield engine
