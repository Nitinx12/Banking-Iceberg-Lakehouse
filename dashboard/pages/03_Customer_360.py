import streamlit as st

st.set_page_config(page_title="Customer 360", layout="wide")
st.title("👤 Customer 360")
st.caption(
    "Masked customer profile, accounts, activity timeline (Architecture 14.2 PII masked views)"
)


@st.cache_data(ttl=300)
def lookup(customer_id: str):
    from dashboard.lib.queries import exec_query

    profile = exec_query(
        "SELECT customer_id, name FROM serving.dim_customer "
        "WHERE customer_id = :cid AND is_current",
        params={"cid": customer_id},
    )
    accounts = exec_query(
        "SELECT account_id, account_type, status, balance FROM serving.dim_account "
        "WHERE customer_id = :cid AND is_current",
        params={"cid": customer_id},
    )
    activity = exec_query(
        """
        SELECT t.txn_date, t.amount, t.channel
        FROM serving.fct_transactions t
        JOIN serving.dim_account a ON t.account_id = a.account_id AND a.is_current
        WHERE a.customer_id = :cid
        ORDER BY t.txn_date DESC
        LIMIT 100
        """,
        params={"cid": customer_id},
    )
    return profile, accounts, activity


customer_id = st.text_input("Customer ID", placeholder="cust_001")
if customer_id:
    try:
        profile, accounts, activity = lookup(customer_id)
        if profile.empty:
            st.warning(f"No customer found for id {customer_id!r}.")
        else:
            p = profile.iloc[0]
            st.subheader(f"Customer {p['customer_id']}")
            # name masked at Silver per Architecture 14.2 — dashboard never sees raw PII
            st.write(f"**Name:** {p['name']}")

            st.subheader("Accounts")
            if accounts.empty:
                st.info("No accounts for this customer.")
            else:
                c1, c2 = st.columns(2)
                c1.metric("Accounts", len(accounts))
                c2.metric("Total Balance", f"₹ {accounts['balance'].sum():,.0f}")
                st.dataframe(accounts, use_container_width=True, hide_index=True)

            st.subheader("Recent activity (latest 100)")
            if activity.empty:
                st.info("No transactions for this customer's accounts.")
            else:
                st.dataframe(activity, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Serving layer not reachable yet — run the daily pipeline first: {e}")
else:
    st.info("Enter a customer ID to load their profile. Data comes from masked serving views.")
