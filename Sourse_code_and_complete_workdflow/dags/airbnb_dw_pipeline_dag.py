"""
airbnb_dw_pipeline_dag.py
Apache Airflow DAG for Coursework 1 — Data Engineering.
Student Number: COHNDDS25.1F-024

Orchestrates the full ELT pipeline for the NYC Airbnb Open Data (2019)
dataset: schema creation -> extract -> clean/validate -> transform ->
load dimensions -> load fact -> load aggregate marts -> run analytical
queries. 8 tasks in total (coursework requires a minimum of 5), each with
an explicit dependency edge, including a fan-out after `transform_data`
and a fan-in before `run_analytical_queries`.

Schedule: runs once per day at 02:00, mirroring the "daily sales file"
/ "morning pipeline" pattern covered in the course (Session 3 / Topic 3).
"""

from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make the project's scripts/ folder importable from within Airflow workers
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "scripts")
sys.path.append(SCRIPTS_DIR)

from db_utils import run_sql_file          # noqa: E402
import extract as extract_module           # noqa: E402
import clean as clean_module               # noqa: E402
import transform as transform_module       # noqa: E402
import load as load_module                 # noqa: E402

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")

default_args = {
    "owner": "data_engineering_coursework",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-eng-alerts@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airbnb_dw_pipeline",
    description="Ingest, clean, transform and load NYC Airbnb data into PostgreSQL, then run analytics",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["coursework1", "data-engineering", "postgresql"],
) as dag:

    # 1. Create / reset the database schema (staging, core, marts)
    create_schema = PythonOperator(
        task_id="create_schema",
        python_callable=run_sql_file,
        op_kwargs={"path": os.path.join(SQL_DIR, "01_schema.sql")},
    )

    # 2. EXTRACT: load the raw CSV into staging.raw_listings
    extract_raw_data = PythonOperator(
        task_id="extract_raw_data",
        python_callable=extract_module.extract,
    )

    # 3. CLEAN & VALIDATE: dedupe, handle nulls, drop invalid records
    clean_and_validate = PythonOperator(
        task_id="clean_and_validate",
        python_callable=clean_module.clean,
    )

    # 4. TRANSFORM: derived columns, date formatting, aggregation
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_module.transform,
    )

    # 5. LOAD dimensions (must finish before the fact table loads, since the
    #    fact table's foreign keys point at the surrogate keys generated here)
    load_dimensions = PythonOperator(
        task_id="load_dimensions",
        python_callable=load_module.load_dimensions,
    )

    # 6. LOAD fact table (depends on load_dimensions)
    load_fact = PythonOperator(
        task_id="load_fact",
        python_callable=load_module.load_fact,
    )

    # 7. LOAD aggregate marts table (can run in parallel with load_fact —
    #    both only depend on transform_data / load_dimensions having run)
    load_marts = PythonOperator(
        task_id="load_marts",
        python_callable=load_module.load_marts,
    )

    # 8. Run the analytical SQL queries once both the fact table and the
    #    marts table have been fully loaded (fan-in)
    def _run_analytical_queries():
        run_sql_file(os.path.join(SQL_DIR, "02_analytical_queries.sql"))

    run_analytical_queries = PythonOperator(
        task_id="run_analytical_queries",
        python_callable=_run_analytical_queries,
    )

    # ---- Task dependencies -------------------------------------------------
    create_schema >> extract_raw_data >> clean_and_validate >> transform_data
    transform_data >> load_dimensions >> load_fact
    load_dimensions >> load_marts
    [load_fact, load_marts] >> run_analytical_queries
