import streamlit as st

st.set_page_config(page_title="Lineage & Docs", layout="wide")
st.title("Lineage & Docs")
st.markdown("""
- **dbt docs** -- `dbt docs generate` -> `target/` (Phase 2)
- **GX Data Docs** — `gx/docs/` on S3/MinIO (Phase 3)
- **Architecture:** `Architecture.md` | **Plan:** `PROJECT_PLAN.md`
""")
st.link_button("Open dbt docs (local)", "http://localhost:8080", disabled=True)
st.link_button("Open GX Data Docs", "http://localhost:8081", disabled=True)
