"""dashboard/Home.py — HDFC Banking Data Platform entry point (Architecture 8.4).

Run: streamlit run dashboard/Home.py  (or make dashboard / tasks.bat dashboard)
"""

import streamlit as st

st.set_page_config(
    page_title="HDFC Banking — Home",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏦 HDFC Banking Data Platform")
st.caption("MongoDB → Iceberg lakehouse → PostgreSQL serving → Streamlit (Architecture 8.4)")


@st.cache_data(ttl=300)
def _scalar(sql: str):
    from dashboard.lib.queries import exec_query

    return exec_query(sql).iloc[0, 0]


def _fmt(v, prefix="", decimals=0):
    try:
        return f"{prefix}{float(v):,.{decimals}f}"
    except (TypeError, ValueError):
        return "—" if v is None else str(v)


def _metric(sql: str, prefix: str = "", decimals: int = 0) -> str:
    try:
        return _fmt(_scalar(sql), prefix=prefix, decimals=decimals)
    except Exception:
        return "—"


col1, col2, col3, col4 = st.columns(4)
col1.metric(
    "Total Deposits",
    _metric("SELECT COALESCE(SUM(balance), 0) FROM serving.dim_account WHERE is_current", "₹ "),
)
col2.metric("Transactions", _metric("SELECT COUNT(*) FROM serving.fct_transactions"))
col3.metric(
    "Active Customers",
    _metric("SELECT COUNT(DISTINCT customer_id) FROM serving.dim_customer WHERE is_current"),
)
col4.metric(
    "DQ Score (24h)",
    _metric(
        "SELECT ROUND(AVG(pass_pct)::numeric, 1) FROM ops.dq_results "
        "WHERE checked_at > now() - interval '24 hours'",
        decimals=1,
    ),
)

st.info(
    "Metrics read from the `serving` schema via the read-only `streamlit_reader` role. "
    "Dashes mean the daily pipeline hasn't published yet — start the core profile and run "
    "`daily_banking_pipeline`."
)

st.markdown(
    """
**Quick links**
- Architecture: `Architecture.md`
- Plan: `PROJECT_PLAN.md`
- Contracts: `contracts/`
- Gold marts: `dbt/banking_dbt/`
"""
)

with st.expander("How to run"):
    st.code("uv run streamlit run dashboard/Home.py  # or tasks.bat dashboard", language="bash")
