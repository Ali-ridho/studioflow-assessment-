
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

POSTGRES_CONN_ID = "studioflow_postgres"

default_args = {
    "owner": "ali_ridho",
    "retries": 2,                          
    "retry_delay": timedelta(minutes=2),
}


def check_reconciliation_gate(**context):
  
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    records = hook.get_records(
        "SELECT month_start, variance_usd FROM analytics.mart_monthly_revenue "
        "WHERE ABS(variance_usd) > 0.01;"
    )
    if records:
        raise ValueError(
            f"RECONCILIATION GATE FAILED! Bulan berikut variance > $0.01: {records}"
        )
    print("Reconciliation gate PASSED - semua bulan dalam toleransi $0.01")


with DAG(
    dag_id="studioflow_revenue_pipeline",
    default_args=default_args,
    description="Pipeline revenue StudioFlow: staging -> intermediate -> analytics",
    schedule_interval="@daily",     
    start_date=datetime(2026, 9, 1),
    catchup=False,                  
    tags=["studioflow", "revenue", "data-engineering"],
    template_searchpath=["/opt/airflow"],
) as dag:

    # ============================================
    # TAHAP 1: Load CSV -> staging (Python, CDC row-hash)
    # ============================================
    load_staging = BashOperator(
        task_id="load_staging",
        bash_command="cd /opt/airflow && python src/load_staging.py",
        env={
            "DB_HOST": "studioflow-postgres",   
            "DB_PORT": "5432",                   
            "DB_NAME": "studioflow",
            "DB_USER": "postgres",
            "DB_PASSWORD": "studioflow123",
        },
        append_env=True,   
    )

    # ============================================
    # TAHAP 2: Resolve versioning (bisa jalan PARALEL)
    # ============================================
    transform_application = PostgresOperator(
        task_id="transform_application_transactions",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/transform_application_transactions.sql",
    )

    transform_manual = PostgresOperator(
        task_id="transform_manual_adjustments",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/transform_manual_adjustment.sql",
    )

    # ============================================
    # TAHAP 3: Bangun fct_revenue_events
    # ============================================
    build_fct = PostgresOperator(
        task_id="build_fct_revenue_events",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/fct_revenue_events.sql",
    )

    # ============================================
    # TAHAP 4: Mart + DQ (bisa jalan PARALEL)
    # ============================================
    build_mart = PostgresOperator(
        task_id="build_mart_monthly_revenue",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/mart_monthly_revenue.sql",
    )

    build_dq = PostgresOperator(
        task_id="build_dq_revenue_issues",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/dq_revenue_issues.sql",
    )

    # ============================================
    # TAHAP 5: Capacity (independen, bisa jalan kapan saja)
    # ============================================
    build_capacity = PostgresOperator(
        task_id="build_mart_capacity_unified",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="sql/build_mart_capacity_unified.sql",
    )

    # ============================================
    # TAHAP 6: Reconciliation Gate (WAJIB setelah mart selesai)
    # ============================================
    reconciliation_gate = PythonOperator(
        task_id="reconciliation_gate",
        python_callable=check_reconciliation_gate,
    )

    # ============================================
    # DEPENDENCY GRAPH
    # ============================================
    load_staging >> [transform_application, transform_manual]
    [transform_application, transform_manual] >> build_fct
    build_fct >> [build_mart, build_dq]
    build_mart >> reconciliation_gate

    load_staging >> build_capacity