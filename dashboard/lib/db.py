"""dashboard/lib/db.py — PostgreSQL connection for Streamlit (Architecture 8.2-8.4).

Uses serving read-only role `streamlit_reader` via env. No superuser.
"""

import os

import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


@st.cache_resource
def get_engine() -> Engine:
    url = os.getenv(
        "STREAMLIT_DB_URL",
        f"postgresql+psycopg2://{os.getenv('POSTGRES_STREAMLIT_READER_USER', 'streamlit_reader')}:{os.getenv('POSTGRES_STREAMLIT_READER_PASSWORD', 'local_streamlit_reader')}@{os.getenv('POSTGRES_HOST', 'postgres')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_WAREHOUSE_DB', 'banking_dw')}",
    )
    return create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10)


def get_conn():
    return get_engine().connect()
