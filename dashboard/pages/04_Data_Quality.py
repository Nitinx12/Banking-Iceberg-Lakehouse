import streamlit as st

st.set_page_config(page_title="Data Quality", layout="wide")
st.title("Data Quality")
st.caption("Latest DQ score, failing checks, quarantine counts, trend (Architecture 11)")

try:
    from dashboard.lib.queries import exec_query

    st.subheader("ops.dq_results (latest)")
    df = exec_query(
        "SELECT layer, table_name, check_name, status, pass_pct, checked_at FROM ops.dq_results ORDER BY checked_at DESC LIMIT 20"
    )
    st.dataframe(df, use_container_width=True)
except Exception as e:
    st.warning(f"DB not yet available: {e}")
    st.info("Phase 3: GX suites + dq_results + quarantine tables will populate this page.")
