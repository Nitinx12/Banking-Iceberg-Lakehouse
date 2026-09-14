"""src.core — reusable PySpark logic (SCD2, transforms, quality, IO)."""

from src.core.io_utils import ensure_db, log_audit, write_delta, write_quarantine
from src.core.quality_checks import (
    QualityResult,
    check_billing,
    check_subscriptions_cdc,
    check_watch_events,
)
from src.core.scd2 import apply_scd2, build_merge_sql
from src.core.transformations import (
    clean_billing,
    clean_content_catalog,
    clean_watch_events,
    dedupe_on_key,
    normalize_strings,
    standardize_timestamps,
)

__all__ = [
    "QualityResult",
    "apply_scd2",
    "build_merge_sql",
    "check_billing",
    "check_subscriptions_cdc",
    "check_watch_events",
    "clean_billing",
    "clean_content_catalog",
    "clean_watch_events",
    "dedupe_on_key",
    "ensure_db",
    "log_audit",
    "normalize_strings",
    "standardize_timestamps",
    "write_delta",
    "write_quarantine",
]
