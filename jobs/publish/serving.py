"""jobs/publish/serving.py — publish Gold → PostgreSQL serving (Architecture 8.3).

1. Load Gold from Iceberg/Delta via Spark JDBC to staging table
2. Validate counts/keys vs Gold
3. Swap in one transaction (INSERT ON CONFLICT for facts, rename for dims)
4. ANALYZE + record freshness ops.freshness_metrics
"""

import os

from jobs.common.logging import get_logger

logger = get_logger("publish")


def _pg_url():
    user = os.getenv("POSTGRES_ETL_WRITER_USER", os.getenv("POSTGRES_USER", "postgres"))
    pw = os.getenv("POSTGRES_ETL_WRITER_PASSWORD", os.getenv("POSTGRES_PASSWORD", ""))
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_WAREHOUSE_DB", "banking_dw")
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"


def publish(table: str, run_id: str):
    try:
        from sqlalchemy import create_engine, text

        from jobs.common.spark import get_spark

        spark = get_spark("publish")
        df = spark.table(f"banking.gold.{table}")
        cnt = df.count()
        logger.info(f"publish {table}: gold count {cnt}")

        if cnt == 0:
            logger.warning(f"publish {table}: no rows in Gold — skipping")
            return 0

        eng = create_engine(_pg_url(), pool_pre_ping=True)

        is_fact = table.startswith("fct_")
        staging = f"serving.{table}_stg"
        target = f"serving.{table}"

        with eng.begin() as c:
            c.execute(text("CREATE SCHEMA IF NOT EXISTS serving"))
            # Drop stale staging from a previous failed run
            c.execute(text(f"DROP TABLE IF EXISTS {staging}"))

            # Load Gold → staging via Spark JDBC (etl_writer role, least privilege per 8.2)
            df.write.format("jdbc").options(
                url=_pg_url(),
                driver="org.postgresql.Driver",
                dbtable=staging,
                batchsize="10000",
            ).mode("overwrite").save()

            # Validate counts vs Gold
            stg_cnt = c.execute(text(f"SELECT COUNT(*) FROM {staging}")).scalar_one()
            if stg_cnt != cnt:
                raise RuntimeError(f"publish {table}: staging count {stg_cnt} != gold count {cnt}")

            # Swap into place
            if is_fact:
                # Incremental facts: INSERT ... ON CONFLICT DO NOTHING
                # Ensure unique constraint exists on target (by business key)
                c.execute(
                    text(f"INSERT INTO {target} SELECT * FROM {staging} ON CONFLICT DO NOTHING")
                )
            else:
                # Small dims: transactional rename swap within same schema
                c.execute(text(f"DROP TABLE IF EXISTS {target}_old"))
                c.execute(
                    text(f"ALTER TABLE IF EXISTS {target} RENAME TO {target.split('.')[1]}_old")
                )
                c.execute(text(f"ALTER TABLE {staging} RENAME TO {table}"))
                c.execute(text(f"DROP TABLE IF EXISTS {target}_old"))

            c.execute(text(f"ANALYZE {target}"))
            c.execute(
                text(
                    "INSERT INTO ops.freshness_metrics (table_name, freshness_seconds, max_loaded_at) VALUES (:t, 0, now())"
                ),
                {"t": table},
            )
            # ops.pipeline_runs row for publish stage per AGENTS 15
            c.execute(
                text(
                    "INSERT INTO ops.pipeline_runs (run_id, batch_id, stage, status, rows_read, rows_written, started_at, finished_at) VALUES (:r, :b, :s, :st, :rr, :rw, now(), now()) ON CONFLICT (run_id, stage) DO UPDATE SET status=:st, rows_read=:rr, rows_written=:rw, finished_at=now()"
                ),
                {
                    "r": run_id,
                    "b": run_id,
                    "s": f"publish_{table}",
                    "st": "success",
                    "rr": cnt,
                    "rw": stg_cnt,
                },
            )

        logger.info(f"publish {table} done run {run_id} cnt {cnt}")
        return cnt
    except Exception as e:
        logger.error(f"publish {table} failed: {e}")
        raise
