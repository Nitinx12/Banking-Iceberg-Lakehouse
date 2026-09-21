"""jobs/ingestion/bronze.py — batch incremental Bronze ingestion (Architecture 5.2).

Design (PROJECT_PLAN Phase 1):
1. Read last watermark from ops.ingestion_watermarks, apply overlap 10m (watermark.py)
2. Query Mongo via pymongo (secondaryPreferred) with {created_at: {$gt: last - overlap}},
   paged by _id so the driver never holds the whole result set in memory
3. Add lineage columns per Architecture 5.3, write to banking.bronze.<collection>,
   then advance watermark after commit
4. Idempotent re-run: delete by _batch_id before write
5. Full refresh path for branches (150 rows) — replace in one transaction

Type handling: BSON DateTime arrives as a naive datetime (no tz) and JSON fixtures carry
ISO-8601 strings, so the watermark is normalized to an aware UTC datetime before it reaches
the _source_ts TIMESTAMP column — a raw ISO string would fail createDataFrame on first run.
The raw document itself stays in _doc as JSON; Silver owns typed parsing of that payload.

Docs with a missing or unparseable watermark cannot be re-fetched reliably or deduped, so
they go to banking.quarantine.<collection> with the reason (Architecture 11.4) instead of
silently breaking the incremental contract.

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

# Bronze lineage schema (Architecture 5.3) — also used for CREATE TABLE
BRONZE_SCHEMA = (
    "_id STRING, _doc STRING, _op STRING, _source_ts TIMESTAMP, _ingested_at TIMESTAMP, "
    "_batch_id STRING, _run_id STRING, _source_collection STRING, _schema_version STRING, "
    "_doc_hash STRING"
)

# Quarantine schema (Architecture 11.4): raw doc plus the failure reason
QUARANTINE_SCHEMA = (
    "_id STRING, _doc STRING, _source_collection STRING, _batch_id STRING, _run_id STRING, "
    "_ingested_at TIMESTAMP, _dq_rule STRING, _doc_hash STRING"
)

BRONZE_TBLPROPS = (
    "TBLPROPERTIES ('format-version'='2','write.format.default'='parquet',"
    "'write.parquet.compression-codec'='zstd','write.target-file-size-bytes'='134217728',"
    "'write.delete.mode'='merge-on-read','write.update.mode'='merge-on-read',"
    "'write.merge.mode'='merge-on-read','history.expire.max-snapshot-age-ms'='604800000')"
)


def _doc_hash(doc_str: str) -> str:
    return hashlib.sha256(doc_str.encode()).hexdigest()


def _now_utc():
    return datetime.now(UTC)


def _to_source_ts(value):
    """Normalize a watermark value to an aware UTC datetime, or None.

    Handles BSON DateTime (naive -> UTC) and ISO-8601 strings from JSON fixtures.
    """
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt
        except ValueError:
            return None
    return None


def _normalize_id(oid):
    """Flatten Mongo _id variants (ObjectId, extended JSON) to a plain string."""
    if isinstance(oid, ObjectId):
        return str(oid)
    if isinstance(oid, dict) and "$oid" in oid:
        return oid["$oid"]
    if oid is None:
        return None
    return str(oid)


def _page_to_rows(
    page, collection, watermark_field, is_full_refresh, ingested_at, batch_id, run_id
):
    """Convert one fetched page of Mongo docs into rows.

    Returns (rows, quarantine_rows, max_source_ts) where:
      rows            — 10-tuples matching BRONZE_SCHEMA (with _doc_hash lineage, Arch 5.3)
      quarantine_rows — 8-tuples matching QUARANTINE_SCHEMA
    """
    rows = []
    quarantine_rows = []
    max_source_ts = None

    for d in page:
        doc_str = json.dumps(d, default=str, sort_keys=True)
        doc_hash = _doc_hash(doc_str)
        oid = _normalize_id(d.get("_id"))

        # watermark: contract field first, then created_at fallback (profiling docs/profiling.md)
        source_ts = _to_source_ts(d.get(watermark_field) if watermark_field else None)
        if source_ts is None:
            source_ts = _to_source_ts(d.get("created_at"))

        if oid is None:
            quarantine_rows.append(
                (None, doc_str, collection, batch_id, run_id, ingested_at, "missing__id", doc_hash)
            )
            continue
        if source_ts is None and not is_full_refresh:
            # missing or unparseable watermark — would silently break incremental/dedup logic.
            # Full-refresh collections (e.g. branches) legitimately have no watermark.
            quarantine_rows.append(
                (
                    oid,
                    doc_str,
                    collection,
                    batch_id,
                    run_id,
                    ingested_at,
                    "missing_or_unparseable_watermark",
                    doc_hash,
                )
            )
            continue

        op = "snapshot" if is_full_refresh else "insert"
        rows.append(
            (
                oid,
                doc_str,
                op,
                source_ts,
                ingested_at,
                batch_id,
                run_id,
                collection,
                "1.0",
                doc_hash,
            )
        )
        if max_source_ts is None or source_ts > max_source_ts:
            max_source_ts = source_ts

    return rows, quarantine_rows, max_source_ts


def ingest_collection(collection: str, batch_id: str, run_id: str, dry_run: bool = False):
    from jobs.ingestion.watermark import advance_watermark, get_watermark, watermark_filter

    watermark_field = cfg.WATERMARK_FIELDS.get(collection, "created_at")
    is_full_refresh = collection in cfg.FULL_REFRESH_COLLECTIONS or watermark_field is None

    # 1. watermark filter
    last_wm, _ = get_watermark(collection)
    if not is_full_refresh and last_wm is not None:
        overlap_start = watermark_filter(collection)
        filt = {watermark_field: {"$gt": overlap_start}}
        logger.info(
            f"{collection}: incremental since {overlap_start} (last {last_wm}) "
            f"overlap {cfg.INGEST_OVERLAP_MINUTES}m"
        )
    elif is_full_refresh:
        filt = {}
        logger.info(f"{collection}: full refresh")
    else:
        filt = {}
        logger.info(f"{collection}: initial load (no watermark)")

    if MongoClient is None:
        raise RuntimeError("pymongo not installed — uv sync --group ingestion")

    client = MongoClient(
        cfg.MONGO_URI, readPreference="secondaryPreferred", serverSelectionTimeoutMS=5000
    )
    db = client.get_database()
    coll = db.get_collection(collection)

    # schema drift check against contracts/<collection>.yml (Architecture 5.4)
    _check_drift(collection, coll)

    # 2. paged reads — fetch one _id page, convert to rows, release the page.
    #    Never accumulate raw docs: at 2-3M docs a driver-side list would OOM.
    ingested_at = _now_utc()
    rows = []
    quarantine_rows = []
    max_source_ts = None
    total_docs = 0
    quarantined_docs = 0
    page_size = 10000
    next_log = 50000
    last_id = None
    t0 = datetime.now(UTC)
    while True:
        page_filter = filt.copy()
        if last_id is not None:
            page_filter["_id"] = {"$gt": last_id}
        cursor = (
            coll.find(page_filter, batch_size=1000, allow_disk_use=True)
            .sort("_id", 1)
            .limit(page_size)
        )
        page = list(cursor)
        if not page:
            break
        total_docs += len(page)
        page_rows, page_quarantine, page_max_ts = _page_to_rows(
            page, collection, watermark_field, is_full_refresh, ingested_at, batch_id, run_id
        )
        rows.extend(page_rows)
        quarantine_rows.extend(page_quarantine)
        quarantined_docs += len(page_quarantine)
        if page_max_ts is not None and (max_source_ts is None or page_max_ts > max_source_ts):
            max_source_ts = page_max_ts
        last_id = page[-1]["_id"]
        if total_docs >= next_log:
            logger.info(f"{collection}: fetched {total_docs} docs so far ...")
            next_log += 50000
        if len(page) < page_size:
            break
    elapsed = (datetime.now(UTC) - t0).total_seconds()
    logger.info(
        f"{collection}: fetched {total_docs} docs for batch {batch_id} "
        f"in {elapsed:.1f}s ({quarantined_docs} quarantined)"
    )

    if dry_run:
        logger.info(f"{collection}: dry_run — not writing")
        return len(rows), None

    # 3. write to Iceberg — idempotent delete by _batch_id before write, full-refresh replace
    spark = get_spark()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.bronze")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS banking.bronze.{collection} ({BRONZE_SCHEMA}) "
        f"USING iceberg PARTITIONED BY (days(_ingested_at)) {BRONZE_TBLPROPS}"
    )

    if is_full_refresh:
        if rows:
            df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
            # full refresh: replace the table atomically — overwritePartitions would only
            # replace today's day partition, not the whole table
            df.writeTo(f"banking.bronze.{collection}").createOrReplace()
            cnt = len(rows)
        else:
            cnt = 0
    elif rows:
        df = spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        # incremental: DELETE+APPEND are two Iceberg commits; idempotency comes from the
        # _batch_id delete guard rather than a single atomic snapshot
        safe_batch = batch_id.replace("'", "''")
        spark.sql(f"DELETE FROM banking.bronze.{collection} WHERE _batch_id = '{safe_batch}'")
        df.writeTo(f"banking.bronze.{collection}").append()
        cnt = len(rows)
    else:
        cnt = 0
    logger.info(f"{collection}: wrote {cnt} rows to banking.bronze.{collection} batch {batch_id}")

    # 4. quarantine — same delete-by-batch guard for re-run safety
    if quarantine_rows:
        qdf = spark.createDataFrame(quarantine_rows, schema=QUARANTINE_SCHEMA)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS banking.quarantine")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS banking.quarantine.{collection} ({QUARANTINE_SCHEMA}) "
            f"USING iceberg {BRONZE_TBLPROPS}"
        )
        safe_batch = batch_id.replace("'", "''")
        spark.sql(f"DELETE FROM banking.quarantine.{collection} WHERE _batch_id = '{safe_batch}'")
        qdf.writeTo(f"banking.quarantine.{collection}").append()
        logger.warning(f"{collection}: quarantined {len(quarantine_rows)} docs")

    # 5. advance watermark only after a successful commit
    if max_source_ts is not None and not is_full_refresh:
        advance_watermark(collection, max_source_ts, batch_id)
        logger.info(f"{collection}: watermark advanced to {max_source_ts} batch {batch_id}")

    # pipeline_runs metric
    _record_run(run_id, batch_id, f"bronze_{collection}", "success", total_docs, cnt)
    return cnt, max_source_ts


def _check_drift(collection: str, coll):
    """Schema drift detector against contracts/<collection>.yml (Architecture 5.4).

    Fail-closed on missing required fields. Type checks are advisory: Mongo is
    schemaless and numeric-as-string is common in fixtures, so only a genuinely
    non-numeric string in a numeric contract field fails the batch.
    """
    import pathlib

    import yaml

    contract_path = pathlib.Path(f"contracts/{collection}.yml")
    if not contract_path.exists():
        logger.warning(f"{collection}: no contract at {contract_path} — drift check skipped")
        return
    try:
        contract = yaml.safe_load(contract_path.read_text())
        field_defs = {f["name"]: f for f in contract.get("fields", [])}
        required = {n for n, fd in field_defs.items() if fd.get("required")}
        sample = coll.find_one()
        if sample is None:
            return
        keys = set(sample.keys())
        missing_required = required - keys
        if missing_required:
            logger.error(
                f"{collection}: drift — missing required fields {missing_required} — policy fail per contract"
            )
            raise ValueError(f"drift fail for {collection}: missing {missing_required}")
        numeric_types = {"int", "long", "double", "decimal"}
        for fname, fdef in field_defs.items():
            if fname in sample and fdef.get("type") in numeric_types:
                v = sample[fname]
                if isinstance(v, str):
                    try:
                        float(v)
                    except ValueError as e:
                        logger.error(
                            f"{collection}: drift — {fname} expected {fdef['type']} got non-numeric string — fail closed"
                        )
                        raise ValueError(
                            f"drift fail for {collection}: type change {fname} {fdef['type']}->str"
                        ) from e
        new_fields = keys - set(field_defs.keys())
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
                    "INSERT INTO ops.pipeline_runs (run_id, batch_id, stage, status, rows_read, rows_written, started_at, finished_at) VALUES (:r,:b,:s,:st,:rr,:rw, now(), now()) ON CONFLICT (run_id, stage) DO UPDATE SET batch_id=:b, status=:st, rows_read=:rr, rows_written=:rw, finished_at=now()"
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
    ap.add_argument("--collections", nargs="*", help="collections to ingest")
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
        ap.error(
            "nothing to ingest — pass --collections <names> or --all "
            f"(configured: {','.join(cfg.INGEST_COLLECTIONS)})"
        )

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
