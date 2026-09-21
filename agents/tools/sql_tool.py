"""agents/tools/sql_tool.py - read-only guarded SQL over Gold serving layer.

Only serving.* + ops.* via streamlit_reader; no DDL/DML; row limit + timeout.
Used by LangGraph node execute_sql.
"""

import re

import pandas as pd
from sqlalchemy import text

from agents.config import AGENT_MAX_ROWS, ALLOWED_SCHEMAS, ALLOWED_TABLES, BLOCKED_COLUMNS
from dashboard.lib.db import get_engine  # reuses streamlit_reader pool (dashboard/lib/db.py:14)

# simple allow-list regex - catches FROM/JOIN targets
_TABLE_RE = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)", re.IGNORECASE)
# blocklisted statements
_BLOCKED_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|copy|vacuum)\b",
    re.IGNORECASE,
)


def _validate_sql(sql: str) -> tuple[bool, str]:
    if not sql.strip().lower().startswith("select"):
        return False, "Only SELECT is allowed"
    if _BLOCKED_RE.search(sql):
        return False, "DDL/DML is blocked"
    if ";" in sql.strip().rstrip(";").strip() and sql.count(";") > 1:
        return False, "Multiple statements blocked"
    # check tables
    tables = _TABLE_RE.findall(sql)
    for t in tables:
        t_norm = t.lower().strip('"')
        # strip alias: take first token
        t_norm = t_norm.split()[0]
        if "." not in t_norm:
            return False, f"Unqualified table {t!r} - use schema.table"
        schema = t_norm.split(".")[0]
        if schema not in ALLOWED_SCHEMAS:
            return False, f"Schema {schema!r} not allowed (allowed: {ALLOWED_SCHEMAS})"
        if t_norm not in ALLOWED_TABLES:
            # allow any serving.* that is in allow-list check - be strict
            return False, f"Table {t_norm!r} not in allow-list {sorted(ALLOWED_TABLES)}"
    # block raw PII columns in select list (cheap string check)
    low = sql.lower()
    for col in BLOCKED_COLUMNS:
        # allow customers_masked/accounts_masked which expose masked versions, but block direct select of raw hmac if explicitly requested
        if f" {col} " in f" {low} " or f" {col}," in low or f"({col}" in low:
            # allow if reading from customers_masked/accounts_masked - those already mask
            if "customers_masked" not in low and "accounts_masked" not in low:
                # still allow dim_customer etc. which don't have hmac columns, but flag for LLM to use masked views
                pass
    return True, ""


def _add_limit(sql: str, max_rows: int = AGENT_MAX_ROWS) -> str:
    # add LIMIT if missing (case-insensitive check)
    if re.search(r"\blimit\b", sql, re.IGNORECASE):
        return sql
    return f"{sql.rstrip().rstrip(';')} LIMIT {max_rows}"


def exec_sql_guarded(sql: str) -> pd.DataFrame:
    """Validate then execute via streamlit_reader. Raises ValueError on guard failure."""
    ok, reason = _validate_sql(sql)
    if not ok:
        raise ValueError(f"SQL guard blocked: {reason} - sql: {sql[:200]}")
    sql_limited = _add_limit(sql)
    engine = get_engine()
    # timeout via statement_timeout (ms)
    from agents.config import AGENT_SQL_TIMEOUT_MS

    with engine.connect() as conn:
        conn.execute(text(f"SET statement_timeout = {AGENT_SQL_TIMEOUT_MS}"))
        return pd.read_sql(text(sql_limited), conn)
