import streamlit as st

st.set_page_config(page_title="Transactions", layout="wide")
st.title("Transactions")
st.caption("Volume by channel and branch, high value table, live alerts panel (streaming Phase 7)")


@st.cache_data(ttl=300)
def load():
    from dashboard.lib.queries import exec_query

    by_channel = exec_query(
        """
        SELECT COALESCE(channel, 'unknown') AS channel, COUNT(*) AS txn_count, SUM(amount) AS total_amount
        FROM serving.fct_transactions
        GROUP BY 1 ORDER BY txn_count DESC
        """
    )
    daily = exec_query(
        """
        SELECT CAST(txn_date AS date) AS day, COUNT(*) AS txn_count
        FROM serving.fct_transactions
        GROUP BY 1 ORDER BY 1
        """
    )
    high_value = exec_query(
        """
        SELECT transaction_id, account_id, amount, channel, txn_date
        FROM serving.fct_transactions
        ORDER BY amount DESC
        LIMIT 50
        """
    )
    return by_channel, daily, high_value


try:
    by_channel, daily, high_value = load()

    left, right = st.columns(2)
    with left:
        st.subheader("Volume by channel")
        st.bar_chart(by_channel.set_index("channel")["txn_count"] if not by_channel.empty else [])
    with right:
        st.subheader("Daily transaction count")
        st.line_chart(daily.set_index("day")["txn_count"] if not daily.empty else [])

    st.subheader("Top 50 high-value transactions")
    st.dataframe(high_value, use_container_width=True, hide_index=True)

    st.subheader("Live alerts")
    st.info("rt.txn_alerts goes live with the Flink streaming phase (PROJECT_PLAN Phase 7).")
except Exception as e:
    st.warning(f"serving.fct_transactions not reachable yet — run the daily pipeline first: {e}")
