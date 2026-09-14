"""
run_pipeline.py
Standalone runner that executes every pipeline stage in order, useful for
local development and testing before deploying the DAG to Airflow. This is
NOT used by Airflow itself (Airflow calls each stage's function directly,
see dags/airbnb_dw_pipeline_dag.py) — it simply mirrors the same task
sequence so the whole pipeline can be run with a single command:

    export PGPASSWORD=postgres
    python3 scripts/run_pipeline.py
"""

import os
import sys

sys.path.append(os.path.dirname(__file__))
from db_utils import run_sql_file
import extract
import clean
import transform
import load

SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")

if __name__ == "__main__":
    print("=== 1/8 Create schema ===")
    run_sql_file(os.path.join(SQL_DIR, "01_schema.sql"))

    print("=== 2/8 Extract ===")
    extract.extract()

    print("=== 3/8 Clean & validate ===")
    clean.clean()

    print("=== 4/8 Transform ===")
    transform.transform()

    print("=== 5/8 Load dimensions ===")
    load.load_dimensions()

    print("=== 6/8 Load fact ===")
    load.load_fact()

    print("=== 7/8 Load marts ===")
    load.load_marts()

    print("=== 8/8 Run analytical queries ===")
    run_sql_file(os.path.join(SQL_DIR, "02_analytical_queries.sql"))

    print("\nPipeline completed successfully.")
