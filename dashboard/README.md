# Dashboard — `dashboard/` (Streamlit)

Structure follows your `dashboard/` layout (`Home.py` + `.streamlit/` + `lib/` + `pages/`):

```
dashboard/
├── .streamlit/config.toml  theme + server (port 8501)
├── lib/
│   ├── db.py               get_engine() via streamlit_reader role (Architecture 8.2)
│   └── queries.py          @st.cache_data(ttl=300) exec_query()
├── pages/
│   ├── 01_Executive_Overview.py
│   ├── 02_Transactions.py
│   ├── 03_Customer_360.py   masked PII views
│   ├── 04_Data_Quality.py   ops.dq_results + quarantine
│   ├── 05_Pipeline_Health.py ops.pipeline_runs + freshness
│   └── 06_Lineage_Docs.py
├── Home.py                 entry point — streamlit run dashboard/Home.py
└── README.md
```

**Run**

```bash
uv run streamlit run dashboard/Home.py   # or tasks.bat dashboard / make dashboard
# Dashboard connects only via POSTGRES_STREAMLIT_READER_PASSWORD (Architecture 8.2)
```

**Hosting guides** — link live URL here after deploy (Phase 4 `docs/hosting.md`):
- Local: http://localhost:8501
- Staging: `https://…`
- Prod: `https://…`

Queries are cached (`@st.cache_data`) and use a `sqlalchemy` pool. No business logic in app — views in `serving` schema.
