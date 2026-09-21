# ADR 007 — LLM Agent on Gold (LangChain + LangGraph + OpenAI) via `serving.*`

**Status:** Accepted 2026-09-21 (Phase 4+ extension)

**Context:** Gold star schema (`dbt/banking_dbt/models/gold/*:1` — 5 models, `contract: {enforced: true}`, SCD2) is published to Postgres `serving.*` via `jobs/publish/serving.py:68` (read via `dashboard/lib/db.py:14` as `streamlit_reader`). `sql/publish_views.sql:1` documents Postgres cannot read Iceberg `banking.*` directly — LLM must not query Iceberg. PII is HMAC-masked in Silver (`agents/config.py:15` BLOCKED_COLUMNS, `docs/runbooks/alert-freshness.md` etc.), exposed only via `serving.customers_masked`/`accounts_masked`.

**Decision:** Add optional `agents/` layer on **Gold serving only**:
- Stack: `langchain>=0.3` + `langgraph>=0.2` + `langchain-openai` + `openai>=1.5` in `pyproject.toml:agents` (`uv sync --group agents`). Model default `gpt-4o-mini` (`OPENAI_MODEL` env) — cheap, sufficient for NL-to-SQL; upgrade via env only.
- Key handling: `OPENAI_API_KEY` in `.env` only (`.env.example:OPENAI_API_KEY=` placeholder, `.gitignore:13` ignores `.env`, `.githooks/pre-commit` gitleaks blocks commit). `agents/config.py:11` loads via `python-dotenv` + `os.getenv`. CI uses GitHub Environments secrets, never `.env`.
- Graph: `agents/graph.py:1` — LangGraph `StateGraph` `classify -> generate_sql -> guard+execute -> synthesize`. SQL generation is LLM, execution is guarded Python tool `agents/tools/sql_tool.py:18` (allow-list `serving.*`/`ops.*` only, `SELECT` only, `LIMIT AGENT_MAX_ROWS`, `statement_timeout AGENT_SQL_TIMEOUT_MS`, least privilege `streamlit_reader`). Never allow-list raw PII columns — use masked views.
- Surface: `dashboard/pages/06_Agent.py:1` Streamlit page calls `agents.graph.ask()`, shows SQL + table + grounded answer. No writing to lake.

**Alternatives:** Direct Iceberg/Spark from LLM — rejected (cross-engine, needs JDBC catalog creds, violates serving boundary). Local-only `ollama` — viable later for parity, but OpenAI unblocks now; `AGENT` group keeps provider swappable via `OPENAI_API_KEY`/`OPENAI_MODEL`.

**Consequences:**
- `uv sync --group agents` optional — `make up PROFILE=core` and dashboard still work without key; Agent page shows setup help if `OPENAI_API_KEY` missing.
- Gold remains source of truth; agent is read-only and non-blocking. DQ `gate` failures → agent warns "Gold held" vs hallucinating.
- Eval: `agents/` failures are observable via `ops.pipeline_runs` / logs if later promoted to a DAG stage; for now Streamlit + `agents/tools/sql_tool.py` guard logs.
- Cost: `gpt-4o-mini` + `LIMIT 100` + 5k ms timeout bounds spend. No vector DB yet — add `pgvector` later if needed for `docs/` retrieval.

**Refs:** `pyproject.toml:agents`, `.env.example:OPENAI_API_KEY`, `agents/config.py`, `agents/graph.py`, `agents/tools/sql_tool.py`, `sql/publish_views.sql`, `dashboard/lib/db.py:14`, `Architecture.md 8.2-8.4, 14.2`.
