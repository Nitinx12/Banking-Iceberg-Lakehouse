from datetime import datetime

from airflow.decorators import dag, task


@dag(dag_id='sla_monitor', schedule='*/5 * * * *', start_date=datetime(2026,1,1), catchup=False)
def sla_monitor():
    @task
    def check_freshness():
        import os

        from sqlalchemy import create_engine, text
        try:
            eng=create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','postgres')}:{os.getenv('POSTGRES_PASSWORD','')}@{os.getenv('POSTGRES_HOST','postgres')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_WAREHOUSE_DB','banking_dw')}")
            with eng.begin() as c:
                c.execute(text("INSERT INTO ops.freshness_metrics (table_name, freshness_seconds, max_loaded_at) SELECT 'fct_transactions', EXTRACT(EPOCH FROM (now() - max(_ingested_at)))::bigint, max(_ingested_at) FROM banking.bronze.transactions"))
        except Exception as e:
            print(f"sla_monitor skipped: {e}")
    check_freshness()
sla_monitor()
