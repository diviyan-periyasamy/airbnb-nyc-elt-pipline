# NYC Airbnb Data Engineering Pipeline

Coursework 1 (Individual) — Data Engineering.
Student Number: COHNDDS25.1F-024

An ELT pipeline that ingests the **NYC Airbnb Open Data (2019)** CSV dataset
(48,895 listings), cleans and validates it, applies four transformations,
loads it into a normalized PostgreSQL star schema, and is orchestrated end
to end with Apache Airflow.

## 1. Project structure

```
project/
├── data/
│   └── AB_NYC_2019.csv          # source dataset (48,895 rows, 16 columns)
├── sql/
│   ├── 01_schema.sql            # staging + core (star schema) + marts DDL
│   └── 02_analytical_queries.sql# 6 analytical business-insight queries
├── scripts/
│   ├── db_utils.py              # shared PostgreSQL connection helpers
│   ├── extract.py                # EXTRACT: raw CSV -> staging.raw_listings
│   ├── clean.py                  # CLEAN/VALIDATE: dedupe, nulls, invalid rows
│   ├── transform.py              # TRANSFORM: derived columns, dates, aggregation
│   ├── load.py                    # LOAD: dimensions, fact table, marts
│   └── run_pipeline.py           # standalone runner (all stages, no Airflow)
├── dags/
│   └── airbnb_dw_pipeline_dag.py # Airflow DAG (8 tasks, explicit dependencies)
├── report/
│   └── Technical_Report.docx
├── requirements.txt
└── README.md
```

## 2. Dataset

**NYC Airbnb Open Data (2019)** — a public dataset describing every active
Airbnb listing in New York City as of 2019 (host, location, room type,
price, reviews, availability). 48,895 rows, well above the 20,000-row
minimum. Source mirror used for this coursework:
`https://raw.githubusercontent.com/pjournal/boun01g-data-mine-r-s/gh-pages/Assignment/AB_NYC_2019.csv`
(originally published via Kaggle / Inside Airbnb).

## 3. Prerequisites

* Python 3.10+
* PostgreSQL 14+ (running locally or reachable over the network)
* Apache Airflow 2.9+ (only required to run the DAG through the scheduler/UI —
  the pipeline logic itself has no Airflow-specific code, so it can also be
  run directly with `run_pipeline.py`)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## 4. Database setup

1. Create the target database:

   ```bash
   psql -U postgres -c "CREATE DATABASE airbnb_dw;"
   ```

2. Set connection environment variables (defaults shown):

   ```bash
   export PG_HOST=localhost
   export PG_PORT=5432
   export PG_DB=airbnb_dw
   export PG_USER=postgres
   export PG_PASSWORD=postgres
   ```

The schema itself (`sql/01_schema.sql`) is applied automatically by the
first pipeline task — you do not need to run it by hand.

## 5. Running the pipeline

### Option A — standalone (no Airflow), fastest way to verify everything works

```bash
cd project
export PGPASSWORD=postgres
python3 scripts/run_pipeline.py
```

This runs all 8 stages in order and prints row counts at every step.

### Option B — through Apache Airflow

1. Point Airflow at this repo's `dags/` folder (e.g. set `dags_folder` in
   `airflow.cfg`, or copy `dags/airbnb_dw_pipeline_dag.py` and the
   `scripts/`, `sql/`, `data/` folders into your `AIRFLOW_HOME/dags/` — the
   DAG imports the scripts as regular Python modules, so their relative
   layout must be preserved).
2. Make sure the `PG_*` environment variables above are available to the
   Airflow worker process (e.g. via `airflow.cfg`, `.env`, or Airflow
   Connections/Variables).
3. Start Airflow:

   ```bash
   airflow db migrate
   airflow standalone
   ```

4. In the Airflow UI (`http://localhost:8080`), un-pause the
   `airbnb_dw_pipeline` DAG and trigger a run.

The DAG's task graph:

```
create_schema >> extract_raw_data >> clean_and_validate >> transform_data
transform_data >> load_dimensions >> load_fact
load_dimensions >> load_marts
[load_fact, load_marts] >> run_analytical_queries
```

## 6. Verifying the results

```bash
psql -U postgres -d airbnb_dw -f sql/02_analytical_queries.sql
```

Or query the star schema directly, e.g.:

```sql
SELECT n.neighbourhood_group, COUNT(*), ROUND(AVG(f.price),2) AS avg_price
FROM core.fact_listings f
JOIN core.dim_neighbourhood n USING (neighbourhood_key)
GROUP BY 1 ORDER BY 2 DESC;
```

## 7. Re-running / idempotency

`01_schema.sql` drops and recreates the `staging`, `core`, and `marts`
schemas from scratch, so the whole pipeline can safely be re-run end to end
(each DAG run is idempotent with respect to the source CSV). For an
incremental/production version, the LOAD stage would be changed to an
upsert (`INSERT ... ON CONFLICT DO UPDATE`) keyed on `listing_id` instead of
appending after a full schema rebuild.

## 8. Author

Coursework 1 — Data Engineering module, 2026.
