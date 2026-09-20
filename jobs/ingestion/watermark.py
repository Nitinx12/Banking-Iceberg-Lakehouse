"""jobs/ingestion/watermark.py — ops.ingestion_watermarks helper (Architecture 5.2, PROJECT_PLAN Phase 1).

Idempotency: watermark only advances after successful Bronze commit. Overlap window protects late commits.
"""

import os
from datetime import datetime, timedelta

from jobs.common import config as cfg


def _engine():
    try:
        from sqlalchemy import create_engine as _ce
    except ImportError:
        return None
    user = os.getenv("POSTGRES_USER", "postgres")
    pw = os.getenv("POSTGRES_PASSWORD", "")
    host = cfg.POSTGRES_HOST
    port = cfg.POSTGRES_PORT
    db = cfg.POSTGRES_WAREHOUSE_DB
    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
    return _ce(url, pool_pre_ping=True)


def get_watermark(collection: str):
    eng = _engine()
    if eng is None:
        return None, None
    try:
        from sqlalchemy import text as _text

        with eng.connect() as conn:
            row = conn.execute(
                _text(
                    "SELECT last_watermark, last_batch_id FROM ops.ingestion_watermarks WHERE source_collection=:c"
                ),
                {"c": collection},
            ).fetchone()
            if row is None:
                return None, None
            return row[0], row[1]
    except Exception:
        return None, None


def advance_watermark(collection: str, new_watermark: datetime, batch_id: str):
    eng = _engine()
    if eng is None:
        return
    try:
        from sqlalchemy import text as _text

        with eng.begin() as conn:
            conn.execute(
                _text(
                    """
                    INSERT INTO ops.ingestion_watermarks (source_collection, last_watermark, last_batch_id, updated_at)
                    VALUES (:c, :w, :b, now())
                    ON CONFLICT (source_collection) DO UPDATE SET last_watermark=:w, last_batch_id=:b, updated_at=now()
                    """
                ),
                {"c": collection, "w": new_watermark, "b": batch_id},
            )
    except Exception:
        pass


def watermark_filter(collection: str):
    wm, _ = get_watermark(collection)
    if wm is None:
        return None
    overlap = timedelta(minutes=cfg.INGEST_OVERLAP_MINUTES)
    return wm - overlap
