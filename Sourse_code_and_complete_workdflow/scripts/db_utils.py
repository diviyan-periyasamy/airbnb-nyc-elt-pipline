"""
db_utils.py
Shared PostgreSQL connection helper used by every stage of the pipeline
(extract, clean, transform, load) and by the Airflow DAG.

Connection settings are read from environment variables so the same code
runs unchanged locally, in Docker, and inside Airflow:

    PG_HOST      (default: localhost)
    PG_PORT      (default: 5432)
    PG_DB        (default: airbnb_dw)
    PG_USER      (default: postgres)
    PG_PASSWORD  (default: postgres)
"""

import os
import psycopg2
from sqlalchemy import create_engine


def get_pg_config() -> dict:
    return {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": os.getenv("PG_PORT", "5432"),
        "dbname": os.getenv("PG_DB", "airbnb_dw"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", "postgres"),
    }


def get_connection():
    """Return a raw psycopg2 connection (used for DDL / executing .sql files)."""
    cfg = get_pg_config()
    return psycopg2.connect(
        host=cfg["host"], port=cfg["port"], dbname=cfg["dbname"],
        user=cfg["user"], password=cfg["password"],
    )


def get_engine():
    """Return a SQLAlchemy engine (used for pandas.to_sql / read_sql)."""
    cfg = get_pg_config()
    url = f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    return create_engine(url)


def run_sql_file(path: str):
    """Execute a .sql file against the target database (used to create the schema)."""
    conn = get_connection()
    conn.autocommit = True
    try:
        with open(path, "r") as f:
            sql = f.read()
        with conn.cursor() as cur:
            cur.execute(sql)
        print(f"[db_utils] Executed SQL file: {path}")
    finally:
        conn.close()
