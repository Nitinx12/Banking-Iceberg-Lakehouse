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

col1, col2, col3, col4 = st.columns(4)
col1.metric("Deposits (placeholder)", "—")
col2.metric("Transactions", "—")
col3.metric("Active Customers", "—")
col4.metric("DQ Score", "—")

st.info(
    "This is the Phase 0 scaffold. Pages in `dashboard/pages/` will show "
    "Executive Overview, Transactions, Customer 360, Data Quality and Pipeline Health once Phase 4 serving is live."
)

st.markdown("""
**Quick links**
- Architecture: `Architecture.md`
- Plan: `PROJECT_PLAN.md`
- Contracts: `contracts/`
- Gold marts: `dbt/banking_dbt/` (Phase 2)
""")

with st.expander("How to run"):
    st.code("uv run streamlit run dashboard/Home.py  # or tasks.bat dashboard", language="bash")
