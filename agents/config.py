"""agents/config.py - env + guardrails for Gold agent (ADR 007)."""

import os

from dotenv import load_dotenv

load_dotenv()  # reads .env at repo root; .env is gitignored per .gitignore:13


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


OPENAI_API_KEY = env("OPENAI_API_KEY", "")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-4o-mini")
AGENT_MAX_ROWS = int(env("AGENT_MAX_ROWS", "100"))
AGENT_SQL_TIMEOUT_MS = int(env("AGENT_SQL_TIMEOUT_MS", "5000"))

# Only these schemas/tables are queryable - least privilege (Architecture 8.2, 14.2)
ALLOWED_SCHEMAS = {"serving", "ops"}
ALLOWED_TABLES = {
    "serving.dim_customer",
    "serving.dim_account",
    "serving.dim_branch",
    "serving.fct_transactions",
    "serving.fct_card_transactions",
    "serving.customers_masked",
    "serving.accounts_masked",
    "ops.dq_results",
    "ops.freshness_metrics",
    "ops.sla_events",
    "ops.pipeline_runs",
}
# Raw PII columns never exposed - Silver holds HMAC only (sql/publish_views.sql:1)
BLOCKED_COLUMNS = {
    "email",
    "phone",
    "email_hmac",
    "phone_hmac",
}  # HMAC also blocked from LLM output - use masked views
# Use customers_masked/accounts_masked for display masking (left(sk,8)||'***')
