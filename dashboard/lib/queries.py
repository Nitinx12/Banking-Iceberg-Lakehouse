"""dashboard/lib/queries.py — cached queries for dashboard pages (Architecture 8.4)."""

import pandas as pd
import streamlit as st

from dashboard.lib.db import get_engine


@st.cache_data(ttl=300)
def exec_query(sql: str, params: dict | None = None) -> pd.DataFrame:
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(sql, conn, params=params)
