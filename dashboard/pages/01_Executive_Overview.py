import streamlit as st

st.set_page_config(page_title="Executive Overview", layout="wide")
st.title("📊 Executive Overview")
st.caption(
    "Deposits, transaction volume, active customers, loan portfolio, trends (Architecture 8.4)"
)


@st.cache_data(ttl=300)
def load():
    from dashboard.lib.queries import exec_query

    kpis = exec_query(
        """
        SELECT
          (SELECT COALESCE(SUM(balance), 0) FROM serving.dim_account WHERE is_current) AS total_deposits,
          (SELECT COUNT(*) FROM serving.fct_transactions) AS txn_count,
          (SELECT COUNT(DISTINCT customer_id) FROM serving.dim_customer WHERE is_current) AS active_customers,
          (SELECT COALESCE(AVG(pass_pct), 0) FROM ops.dq_results
            WHERE checked_at > now() - interval '24 hours') AS dq_score_24h
        """
    )
    trend = exec_query(
        """
        SELECT CAST(txn_date AS date) AS day, COUNT(*) AS txn_count, SUM(amount) AS txn_amount
        FROM serving.fct_transactions
        GROUP BY 1 ORDER BY 1
        """
    )
    by_type = exec_query(
        """
        SELECT account_type, COUNT(*) AS accounts, SUM(balance) AS total_balance
        FROM serving.dim_account WHERE is_current
        GROUP BY 1 ORDER BY total_balance DESC
        """
    )
    return kpis, trend, by_type


def _v(row, col):
    try:
        return row.iloc[0][col]
    except Exception:
        return None


try:
    kpis, trend, by_type = load()
    k = kpis.iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Deposits", f"₹ {float(k['total_deposits'] or 0):,.0f}")
    c2.metric("Transactions", f"{int(k['txn_count'] or 0):,}")
    c3.metric("Active Customers", f"{int(k['active_customers'] or 0):,}")
    dq = k["dq_score_24h"]
    c4.metric("DQ Score (24h)", "—" if dq is None else f"{float(dq):.1f}%")

    left, right = st.columns(2)
    with left:
        st.subheader("Transaction volume")
        if trend.empty:
            st.info("No transactions published yet.")
        else:
            st.bar_chart(trend.set_index("day"))
    with right:
        st.subheader("Deposits by account type")
        if by_type.empty:
            st.info("No accounts published yet.")
        else:
            st.bar_chart(by_type.set_index("account_type")["total_balance"])
            st.dataframe(by_type, use_container_width=True)
except Exception as e:
    st.warning(
        f"Serving layer not reachable yet — start the core profile and run the pipeline: {e}"
    )
