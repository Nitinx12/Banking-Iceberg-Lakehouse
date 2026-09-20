import streamlit as st

st.set_page_config(page_title="Pipeline Health", layout="wide")
st.title("🔧 Pipeline Health")
st.caption("Run history, freshness per table, SLA/SLO status, error budget (Architecture 13)")

try:
    from dashboard.lib.queries import exec_query

    st.subheader("ops.pipeline_runs")
    df = exec_query(
        "SELECT run_id, stage, status, rows_read, rows_written, started_at FROM ops.pipeline_runs ORDER BY started_at DESC LIMIT 20"
    )
    st.dataframe(df, use_container_width=True)

    st.subheader("ops.freshness_metrics")
    df2 = exec_query(
        "SELECT table_name, freshness_seconds, max_loaded_at, checked_at FROM ops.freshness_metrics ORDER BY checked_at DESC LIMIT 20"
    )
    st.dataframe(df2, use_container_width=True)
except Exception as e:
    st.warning(f"DB not yet available: {e}")
    st.info("Phase 5: sla_monitor DAG + freshness_metrics + Grafana will feed this page.")
