import streamlit as st

st.set_page_config(page_title="Customer 360", layout="wide")
st.title("👤 Customer 360")
st.caption(
    "Masked customer profile, accounts, activity timeline (Architecture 14.2 PII masked views)"
)

customer_id = st.text_input("Customer ID", placeholder="cust_001")
if customer_id:
    st.info(f"Placeholder — will query masked views in `serving` for {customer_id}")
