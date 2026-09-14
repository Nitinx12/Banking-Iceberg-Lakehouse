"""Databricks connection helpers — SQL warehouse + workspace client.

Uses .env via src.config (DATABRICKS_HOST/TOKEN/HTTP_PATH). Falls back to
Databricks runtime (dbutils.secrets) when on cluster.
"""

from __future__ import annotations

from typing import Any


def get_workspace_client() -> Any:
    """Return databricks-sdk WorkspaceClient if available, else None."""
    try:
        from databricks.sdk import WorkspaceClient

        from src.config import get_config

        cfg = get_config()
        # SDK picks up DATABRICKS_HOST/TOKEN from env automatically
        return WorkspaceClient(host=cfg.databricks_host, token=cfg.databricks_token)
    except Exception as e:
        # Optional dep — not required for local PySpark tests
        import logging

        logging.getLogger("streamflix").debug("WorkspaceClient unavailable: %s", e)
        return None


def get_sql_connection() -> Any:
    """Return a databricks-sql-connector Connection for the warehouse.

    Requires DATABRICKS_HOST, DATABRICKS_HTTP_PATH, DATABRICKS_TOKEN in .env.
    Install: `uv add databricks-sql-connector` (optional for local dev).
    """
    try:
        from databricks import sql as dbsql  # type: ignore[import-untyped]

        from src.config import get_config

        cfg = get_config()
        if (
            not cfg.databricks_host
            or not cfg.databricks_http_path
            or not cfg.databricks_token
        ):
            raise RuntimeError("Missing DATABRICKS_HOST/HTTP_PATH/TOKEN — check .env")
        host = cfg.databricks_host.replace("https://", "").rstrip("/")
        return dbsql.connect(
            server_hostname=host,
            http_path=cfg.databricks_http_path,
            access_token=cfg.databricks_token,
        )
    except ImportError as e:
        raise ImportError(
            "databricks-sql-connector not installed — run `uv add databricks-sql-connector`"
        ) from e
