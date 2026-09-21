"""dashboard/pages/06_Agent.py - Gold agent (LangGraph + OpenAI) over serving.* (ADR 007).

Reads only from serving.* via streamlit_reader (dashboard/lib/db.py:14) and
agents/tools/sql_tool.py guard. No raw PII - HMAC masked views only.
Requires OPENAI_API_KEY in .env (see .env.example). Without it, page shows setup help.
"""

import os

import streamlit as st

st.set_page_config(page_title="Gold Agent", layout="wide")
st.title("Gold Agent - ask the lakehouse")
st.caption(
    "LangChain + LangGraph on Gold serving layer (Postgres `serving.*` + `ops.*`, read-only, HMAC-masked)"
)

has_key = bool(os.getenv("OPENAI_API_KEY", ""))

with st.expander("Setup - do once", expanded=not has_key):
    st.markdown(
        """
**1. Add to `.env` (gitignored, never commit):**
```bash
# in .env at repo root
OPENAI_API_KEY=sk-proj-...your key...
OPENAI_MODEL=gpt-4o-mini
```
Get a key: https://platform.openai.com/api-keys

**2. Install agent deps:**
```bash
uv sync --group agents   # or: uv sync --group dashboard --group agents
# Windows: tasks.bat setup  then add --group agents
```

**3. Run:**
```bash
uv run streamlit run dashboard/Home.py
# open Pages -> Agent
```

Key is read via `agents/config.py:11` (`python-dotenv` + `os.getenv`). It is blocked from commits by `.githooks/pre-commit` gitleaks and `.gitignore:13`.
"""
    )
    if not has_key:
        st.warning("`OPENAI_API_KEY` not found in env - paste it in `.env` and restart Streamlit.")
    else:
        st.success(
            f"Key found - model `{os.getenv('OPENAI_MODEL', 'gpt-4o-mini')}`. Try a question below."
        )

examples = [
    "total deposits by branch, top 5",
    "how many transactions last 7 days?",
    "customer 360 for customer_id 123",
    "what is the DQ score last 24h and any critical failures?",
    "which accounts have balance > 1M?",
]

col1, col2 = st.columns([3, 1])
with col2:
    st.write("Examples - click to fill:")
    for ex in examples:
        if st.button(ex, key=f"ex_{ex}", use_container_width=True):
            st.session_state["agent_q"] = ex

q = st.text_input(
    "Ask Gold",
    key="agent_q",
    placeholder="e.g. total deposits by branch, top 5",
)

run = st.button("Ask", type="primary", disabled=not q.strip())


@st.cache_data(ttl=60, show_spinner=False)
def _cached_ask(question: str) -> dict:
    from agents.graph import ask

    return ask(question)


if run and q.strip():
    with st.spinner("Generating SQL -> executing on serving.* -> synthesizing..."):
        try:
            result = _cached_ask(q.strip())
        except Exception as e:
            st.error(f"Agent failed: {e}")
            st.stop()

        if result.get("error"):
            st.error(result["error"])
        st.code(result.get("sql", ""), language="sql")

        rows = result.get("rows") or []
        if rows:
            import pandas as pd

            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.caption(
                f"{result.get('row_count', len(rows))} rows (limit {os.getenv('AGENT_MAX_ROWS', '100')})"
            )
        else:
            st.info("No rows returned for that query.")

        st.markdown("**Answer**")
        st.write(result.get("answer", ""))

st.divider()
st.caption(
    "Guardrails: only `serving.*` + `ops.*` via `agents/tools/sql_tool.py:18` (allow-list, LIMIT, statement_timeout). "
    "Raw email/phone never leaves Postgres - use `serving.customers_masked` / `serving.accounts_masked`."
)
