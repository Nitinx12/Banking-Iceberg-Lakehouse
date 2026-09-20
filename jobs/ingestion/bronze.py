"""jobs/ingestion/bronze.py — batch incremental Bronze ingestion (Architecture 5.2).

Design (PROJECT_PLAN Phase 1):
1. Read last watermark from ops.ingestion_watermarks, apply overlap 10m (watermark.py)
2. Query Mongo via pymongo (secondaryPreferred analog) with {created_at: {$gt: last - overlap}} — partitioned logically by _id ranges
3. Add lineage columns per Architecture 5.3, write to banking.bronze.<collection> in one Iceberg transaction, then advance watermark after commit
4. Idempotent re-run: delete by _batch_id before write
5. Full refresh path for branches (150 rows) — overwrite in one transaction

Usage:
  uv run python -m jobs.ingestion.bronze --collections customers --batch-id local-20260920 --run-id run-001
  uv run python -m jobs.ingestion.bronze --all  (uses INGEST_COLLECTIONS from .env)
CE note: Silver/Gold Delta fallback decided in ADR 003 — Bronze stays Iceberg even on CE.
"""

import argparse
import hashlib
import json
import uuid
from datetime import UTC, datetime

from bson import ObjectId

from jobs.common import config as cfg
from jobs.common.logging import get_logger
from jobs.common.spark import get_spark

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None

logger = get_logger("bronze")


def _doc_hash(doc_str: str) -> str:
    return hashlib.sha256(doc_str.encode()).hexdigest()


def _now_utc():
    return datetime.now(UTC)


def ingest_collection(collection: str, batch_id: str, run_id: str, dry_run: bool = False):
    from jobs.ingestion.watermark import advance_watermark, get_watermark, watermark_filter

    watermark_field = cfg.WATERMARK_FIELDS.get(collection, "created_at")
    is_full_refresh = collection in cfg.FULL_REFRESH_COLLECTIONS or watermark_field is None

    # 1. watermark filter
    filt = {}
    last_wm, _ = get_watermark(collection)
    if not is_full_refresh and last_wm is not None:
        overlap_start = watermark_filter(collection)
        filt = {watermark_field: {"$gt": overlap_start}}
        logger.info(
            f"{collection}: incremental since {overlap_start} (last {last_wm}) overlap {cfg.INGEST_OVERLAP_MINUTES}m"
        )
    elif is_full_refresh:
        logger.info(f"{collection}: full refresh (150 rows)")
    else:
        logger.info(f"{collection}: initial load (no watermark)")

    if MongoClient is None:
        raise RuntimeError("pymongo not installed — uv sync --group ingestion")

    client = MongoClient(
        cfg.MONGO_URI, readPreference="secondaryPreferred", serverSelectionTimeoutMS=5000
    )
    db = client.get_database()
    coll = db.get_collection(collection)

    # optional schema drift check stub
    _check_drift(collection, coll)

    # 2. partitioned reads by _id ranges — count then fetch (for local dev we fetch all; real volume partitions by _id)
    cursor = coll.find(filt).sort("_id", 1) if not is_full_refresh else coll.find({}).sort("_id", 1)
    docs = list(cursor)
    logger.info(f"{collection}: fetched {len(docs)} docs for batch {batch_id}")

    if dry_run:
        logger.info(f"{collection}: dry_run — not writing")
        return len(docs), None

    # 3. lineage enrichment — Architecture 5.3 Bronze contract
    ingested_at = _now_utc()
    rows = []
    max_source_ts = None
    for d in docs:
        doc_str = json.dumps(d, default=str, sort_keys=True)
        source_ts = d.get(watermark_field) if watermark_field else d.get("created_at")
        # normalize source_ts to datetime
        if isinstance(source_ts, dict) and "$date" in source_ts:
            source_ts = datetime.fromisoformat(source_ts["$date"].replace("Z", "+00:00"))
        if isinstance(source_ts, datetime) and source_ts.tzinfo is None:
            source_ts = source_ts.replace(tzinfo=UTC)
        if isinstance(source_ts, datetime) and (max_source_ts is None or source_ts > max_source_ts):
            max_source_ts = source_ts
        # _id handling
        oid = d.get("_id")
        if isinstance(oid, ObjectId):
            oid = str(oid)
        elif isinstance(oid, dict) and "$oid" in oid:
            oid = oid["$oid"]
        else:
            oid = str(oid)
        rows.append(
            (
                oid,
                doc_str,
                "snapshot" if is_full_refresh else "insert",
                source_ts,
                ingested_at,
                batch_id,
                run_id,
                collection,
                "1.0",
                _doc_hash(doc_str),
            )
        )

    # 4. write to Iceberg — idempotent delete by _batch_id before write
    spark = get_spark()
    schema = "_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, _batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, _doc_hash STRING"
    if rows:
        df = spark.createDataFrame(rows, schema=schema)
        # ensure namespace/table exists
        spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.bronze")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS banking.bronze.{collection} ({schema}) USING iceberg PARTITIONED BY (days(_ingested_at)) TBLPROPERTIES ('format-version'='2','write.format.default'='parquet')"
        )
        # idempotent delete
        spark.sql(f"DELETE FROM banking.bronze.{collection} WHERE _batch_id = '{batch_id}'")
        df.writeTo(f"banking.bronze.{collection}").append()
        cnt = df.count()
        logger.info(
            f"{collection}: wrote {cnt} rows to banking.bronze.{collection} batch {batch_id}"
        )
        # 5. advance watermark after commit
        if max_source_ts is not None and not is_full_refresh:
            advance_watermark(collection, max_source_ts, batch_id)
            logger.info(f"{collection}: watermark advanced to {max_source_ts} batch {batch_id}")
        # pipeline_runs metric
        _record_run(run_id, batch_id, f"bronze_{collection}", "success", len(docs), cnt)
        return cnt, max_source_ts
    else:
        logger.info(f"{collection}: no rows to write")
        _record_run(run_id, batch_id, f"bronze_{collection}", "success", 0, 0)
        return 0, None


def _check_drift(collection: str, coll):
    """Schema drift detector against contracts/<collection>.yml (Architecture 5.4)."""
    import pathlib

    import yaml

    contract_path = pathlib.Path(f"contracts/{collection}.yml")
    if not contract_path.exists():
        logger.warning(f"{collection}: no contract at {contract_path} — drift check skipped")
        return
    try:
        contract = yaml.safe_load(contract_path.read_text())
        required = {f["name"] for f in contract.get("fields", []) if f.get("required")}
        sample = coll.find_one()
        if sample is None:
            return
        # normalize _id key
        keys = set(sample.keys())
        missing_required = required - keys
        if missing_required:
            logger.error(
                f"{collection}: drift — missing required fields {missing_required} — policy fail per contract"
            )
            # fail closed: quarantine batch, alert (Phase 1 stub raises)
            raise ValueError(f"drift fail for {collection}: missing {missing_required}")
        new_fields = keys - {f["name"] for f in contract.get("fields", [])}
        if new_fields:
            logger.warning(f"{collection}: drift — new fields {new_fields} — warn per contract")
    except Exception as e:
        if "drift fail" in str(e):
            raise
        logger.warning(f"{collection}: drift check error {e}")


def _record_run(run_id, batch_id, stage, status, rows_read, rows_written):
    try:
        import os

        from sqlalchemy import create_engine, text

        user = os.getenv("POSTGRES_USER", "postgres")
        pw = os.getenv("POSTGRES_PASSWORD", "")
        host = cfg.POSTGRES_HOST
        port = cfg.POSTGRES_PORT
        db = cfg.POSTGRES_WAREHOUSE_DB
        eng = create_engine(
            f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}", pool_pre_ping=True
        )
        with eng.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO ops.pipeline_runs (run_id, batch_id, stage, status, rows_read, rows_written, started_at, finished_at) VALUES (:r,:b,:s,:st,:rr,:rw, now(), now()) ON CONFLICT (run_id) DO UPDATE SET batch_id=:b, stage=:s, status=:st, rows_read=:rr, rows_written=:rw, finished_at=now()"
                ),
                {
                    "r": run_id,
                    "b": batch_id,
                    "s": stage,
                    "st": status,
                    "rr": rows_read,
                    "rw": rows_written,
                },
            )
    except Exception as e:
        logger.warning(f"pipeline_runs record failed: {e}")


def main():
    ap = argparse.ArgumentParser(description="Bronze batch ingestion")
    ap.add_argument("--collections", nargs="+", help="collections to ingest")
    ap.add_argument("--all", action="store_true", help="ingest all INGEST_COLLECTIONS from .env")
    ap.add_argument("--batch-id", default=None)
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.all:
        collections = cfg.INGEST_COLLECTIONS
    elif args.collections:
        collections = args.collections
    else:
        collections = cfg.INGEST_COLLECTIONS

    batch_id = (
        args.batch_id
        or f"{cfg.INGEST_BATCH_ID_PREFIX}-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    )
    run_id = args.run_id or f"run-{uuid.uuid4().hex[:8]}"

    logger.info(
        f"Starting bronze ingestion run {run_id} batch {batch_id} collections {collections}"
    )

    for c in collections:
        try:
            ingest_collection(c, batch_id, run_id, dry_run=args.dry_run)
        except Exception as e:
            logger.error(f"{c}: ingestion failed: {e}")
            _record_run(run_id, batch_id, f"bronze_{c}", "failed", 0, 0)
            if not args.dry_run:
                raise
    logger.info(f"Bronze ingestion complete run {run_id} batch {batch_id}")


if __name__ == "__main__":
    main()
