"""Central config — loads .env locally, falls back to Databricks widgets/secrets on cluster.

Interview talking point: `.env` + widgets are a documented stand-in for
Databricks Secrets + cloud KMS (README §3, plan §3). Production upgrade:
  - widgets -> dbutils.secrets.get(scope, key)
  - .env file -> Databricks Secret Scope backed by AWS Secrets Manager / Azure Key Vault
  - CATALOG_* Hive DBs (bronze/silver/gold) -> Unity Catalog catalogs

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
    databricks_cluster_id: str | None
    landing_root: str
    checkpoint_root: str
    catalog_name: str  # tham (Unity Catalog)
    catalog_schema: str  # init_schema (sql/init_schema.sql)
    bronze_db: str
    silver_db: str
    gold_db: str

    @property
    def tham_fqn(self) -> str:
        return f"{self.catalog_name}.{self.catalog_schema}"

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


def get_config() -> Config:
    return Config(
        env=_get("ENV", "dev") or "dev",
        databricks_host=_get("DATABRICKS_HOST"),
        databricks_token=_get("DATABRICKS_TOKEN"),
        databricks_cluster_id=_get("DATABRICKS_CLUSTER_ID"),
        landing_root=_get("LANDING_ROOT", "/dbfs/mnt/landing") or "/dbfs/mnt/landing",
        checkpoint_root=_get("CHECKPOINT_ROOT", "/dbfs/mnt/checkpoints") or "/dbfs/mnt/checkpoints",
        catalog_name=_get("CATALOG_NAME", "tham") or "tham",
        catalog_schema=_get("CATALOG_SCHEMA", "init_schema") or "init_schema",
        bronze_db=_get("CATALOG_BRONZE", "bronze") or "bronze",
        silver_db=_get("CATALOG_SILVER", "silver") or "silver",
        gold_db=_get("CATALOG_GOLD", "gold") or "gold",
    )
