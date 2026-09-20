import streamlit as st

st.set_page_config(page_title="Executive Overview", layout="wide")
st.title("📊 Executive Overview")
st.caption(
    "Deposits, transaction volume, active customers, loan portfolio, trends (Architecture 8.4)"
)

st.info(
    "Placeholder — Phase 4 will query `serving.agg_daily_branch_kpis` and Gold marts. Connects via `streamlit_reader` role only."
)

try:
    from dashboard.lib.queries import exec_query

    df = exec_query("SELECT 1 as placeholder")
    st.dataframe(df, use_container_width=True)
except Exception as e:
    st.warning(f"DB not yet available (core profile not up): {e}")
