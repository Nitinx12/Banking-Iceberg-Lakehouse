FROM apache/airflow:2.9.3-python3.11
# Architecture 9.2 — isolated venv for dbt via Cosmos + local spark jobs
# Install uv, copy pyproject, sync ingestion/transform deps
COPY pyproject.toml uv.lock README.md ./
COPY jobs/ ./jobs/
RUN pip install --no-cache-dir uv && uv sync --group ingestion --group transform || pip install -e .
USER airflow
