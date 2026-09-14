"""Central config — loads .env locally, falls back to Databricks widgets/secrets on cluster.

Supports both legacy vars (CATALOG_BRONZE etc.) and current .env (CATALOG_NAME/BRONZE_SCHEMA etc.).
Interview talking point: `.env` + widgets are a documented stand-in for
Databricks Secrets + cloud KMS (README §3, plan §3). Production upgrade:
  - widgets -> dbutils.secrets.get(scope, key)
  - .env file -> Databricks Secret Scope backed by AWS Secrets Manager / Azure Key Vault
  - Hive DBs (bronze/silver/gold) -> Unity Catalog `streamflix-lakehouse`.{bronze,silver,gold}

Usage on cluster:
  dbutils.widgets.text("env", "dev")
  from src.config import get_config
  cfg = get_config()

Locally:
  from dotenv import load_dotenv  # auto-loaded here
  cfg = get_config()  # reads .env
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

try:
    from dotenv import load_dotenv

    # Load from repo root .env if present; no-op on cluster where file absent
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=False)
except ImportError:
    pass


@dataclass(frozen=True)
class Config:
    env: str
    databricks_host: str | None
    databricks_token: str | None
    databricks_workspace_id: str | None
    databricks_http_path: str | None
    landing_root: str
    checkpoint_root: str
    quarantine_path: str
    catalog_name: str  # streamflix-lakehouse (UC)
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    init_schema: str  # sql/init_schema.sql
    audit_table: str
    lineage_table: str
    quality_threshold: float
    max_fail_count: int
    spark_shuffle_partitions: int
    spark_adaptive_enabled: bool
    log_level: str
    debug_mode: bool

    # Back-compat aliases
    @property
    def bronze_db(self) -> str:
        return self.bronze_schema

    @property
    def silver_db(self) -> str:
        return self.silver_schema

    @property
    def gold_db(self) -> str:
        return self.gold_schema

    @property
    def catalog_schema(self) -> str:
        return self.init_schema

    @property
    def tham_fqn(self) -> str:
        return f"{self.catalog_name}.{self.init_schema}"

    def fqn(self, schema: str, table: str) -> str:
        return f"`{self.catalog_name}`.{schema}.{table}"

    @property
    def is_local(self) -> bool:
        # Databricks runtime sets DATABRICKS_RUNTIME_VERSION
        return "DATABRICKS_RUNTIME_VERSION" not in os.environ


def _get(key: str, default: str | None = None) -> str | None:
    """Try os.environ first, then Databricks widgets/secrets if available."""
    val = os.environ.get(key)
    if val:
        return val
    # On cluster, try widgets
    try:
        import IPython  # noqa: F401

        # dbutils is injected by Databricks runtime, not importable locally
        dbutils = globals().get("dbutils")  # type: ignore[assignment]
        if dbutils is not None:
            try:
                w = dbutils.widgets.get(key.lower())  # type: ignore[union-attr]
                if w:
                    return w
            except Exception:
                pass
    except Exception:
        pass
    return default


def _get_bool(key: str, default: bool) -> bool:
    v = _get(key)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "on")


def _get_int(key: str, default: int) -> int:
    v = _get(key)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _get_float(key: str, default: float) -> float:
    v = _get(key)
    if v is None or v == "":
        return default
    try:
        return float(v)
    except ValueError:
        return default


def get_config() -> Config:
    # Support both old (CATALOG_BRONZE) and new (BRONZE_SCHEMA) var names
    bronze = _get("BRONZE_SCHEMA") or _get("CATALOG_BRONZE", "bronze") or "bronze"
    silver = _get("SILVER_SCHEMA") or _get("CATALOG_SILVER", "silver") or "silver"
    gold = _get("GOLD_SCHEMA") or _get("CATALOG_GOLD", "gold") or "gold"
    init = _get("INIT_SCHEMA") or _get("CATALOG_SCHEMA", "init_schema") or "init_schema"
    catalog = _get("CATALOG_NAME", "streamflix-lakehouse") or "streamflix-lakehouse"
    env = _get("ENVIRONMENT") or _get("ENV", "dev") or "dev"
    return Config(
        env=env,
        databricks_host=_get("DATABRICKS_HOST"),
        databricks_token=_get("DATABRICKS_TOKEN"),
        databricks_workspace_id=_get("DATABRICKS_WORKSPACE_ID"),
        databricks_http_path=_get("DATABRICKS_HTTP_PATH"),
        landing_root=_get("RAW_DATA_PATH") or _get("LANDING_ROOT", "/Volumes/streamflix-lakehouse/bronze/raw_data") or "/Volumes/streamflix-lakehouse/bronze/raw_data",
        checkpoint_root=_get("CHECKPOINT_PATH") or _get("CHECKPOINT_ROOT", "/Volumes/streamflix-lakehouse/bronze/checkpoints") or "/Volumes/streamflix-lakehouse/bronze/checkpoints",
        quarantine_path=_get("QUARANTINE_PATH", "/Volumes/streamflix-lakehouse/silver/quarantine") or "/Volumes/streamflix-lakehouse/silver/quarantine",
        catalog_name=catalog,
        bronze_schema=bronze,
        silver_schema=silver,
        gold_schema=gold,
        init_schema=init,
        audit_table=_get("AUDIT_TABLE", f"{catalog}.silver.audit_log") or f"{catalog}.silver.audit_log",
        lineage_table=_get("LINEAGE_TABLE", f"{catalog}.init_schema.lineage") or f"{catalog}.init_schema.lineage",
        quality_threshold=_get_float("QUALITY_THRESHOLD", 0.95),
        max_fail_count=_get_int("MAX_FAIL_COUNT", 100),
        spark_shuffle_partitions=_get_int("SPARK_SHUFFLE_PARTITIONS", 200),
        spark_adaptive_enabled=_get_bool("SPARK_SQL_ADAPTIVE_ENABLED", True),
        log_level=_get("LOG_LEVEL", "INFO") or "INFO",
        debug_mode=_get_bool("DEBUG_MODE", False),
    )
