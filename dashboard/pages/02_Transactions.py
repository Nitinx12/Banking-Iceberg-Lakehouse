import streamlit as st

st.set_page_config(page_title="Transactions", layout="wide")
st.title("💳 Transactions")
st.caption("Volume by channel and branch, high value table, live alerts panel (streaming Phase 7)")

st.info("Placeholder — will query `serving.fct_transactions` + `rt.txn_alerts` (Flink stretch).")
st.dataframe([], use_container_width=True)
