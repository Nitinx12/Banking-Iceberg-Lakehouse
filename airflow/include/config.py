"""airflow/include/config.py — shared Airflow config per Architecture 18.

Centralizes pools, variables, alert callbacks, dbt selectors.
DAGs import from here instead of hardcoding.
"""

ALERT_RUNBOOK_BASE = "docs/runbooks"
DEFAULT_POOL = "mongo_pool"
DATABRICKS_POOL = "databricks_pool"
SELECTORS = {
    "silver": "silver",
    "gold": "gold",
    "critical": "critical",
}
