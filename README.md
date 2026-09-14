#  NYC Airbnb Data Engineering Pipeline

**Coursework 1 (Individual) — Data Engineering**

An end-to-end **ELT (Extract, Load, Transform) data engineering pipeline** that ingests the **NYC Airbnb Open Data (2019)** dataset containing **48,895 listings**, cleans and validates the data, applies multiple transformations, loads it into a normalized **PostgreSQL star schema**, and orchestrates the complete workflow using **Apache Airflow**.

---

##  Project Overview

This project demonstrates a complete data engineering workflow for transforming raw Airbnb listing data into an analytics-ready data warehouse.

The pipeline performs:

```text
NYC Airbnb CSV Dataset
        │
        ▼
     EXTRACT
        │
        ▼
PostgreSQL Staging
        │
        ▼
 CLEAN & VALIDATE
        │
        ▼
    TRANSFORM
        │
        ▼
PostgreSQL Data Warehouse
        │
        ├── Dimension Tables
        │
        ├── Fact Table
        │
        └── Data Marts
        │
        ▼
 Analytical SQL Queries
        │
        ▼
Business Insights
```

The pipeline can be executed in two ways:

* **Standalone Python pipeline** — without Airflow
* **Apache Airflow DAG** — complete workflow orchestration

---

#  1. Project Structure

```text
project/
│
├── data/
│   └── AB_NYC_2019.csv
│       # Source dataset (48,895 rows, 16 columns)
│
├── sql/
│   ├── 01_schema.sql
│   │   # Staging, core star schema and marts DDL
│   │
│   └── 02_analytical_queries.sql
│       # 6 analytical business-insight queries
│
├── scripts/
│   ├── db_utils.py
│   │   # Shared PostgreSQL connection helpers
│   │
│   ├── extract.py
│   │   # EXTRACT: CSV → staging.raw_listings
│   │
│   ├── clean.py
│   │   # CLEAN / VALIDATE: duplicates, nulls, invalid rows
│   │
│   ├── transform.py
│   │   # TRANSFORM: derived columns, dates and aggregation
│   │
│   ├── load.py
│   │   # LOAD: dimensions, fact table and marts
│   │
│   └── run_pipeline.py
│       # Standalone pipeline runner
│
├── dags/
│   └── airbnb_dw_pipeline_dag.py
│       # Apache Airflow DAG with 8 tasks
│
├── report/
│   └── Technical_Report.docx
│
├── requirements.txt
└── README.md
```

---

#  2. Dataset

The project uses the **NYC Airbnb Open Data (2019)** dataset.

The dataset contains information about active Airbnb listings in New York City, including:

* Listing information
* Host information
* Neighbourhood
* Room type
* Price
* Number of reviews
* Reviews per month
* Availability
* Geographic coordinates

### Dataset Statistics

| Property              |                Value |
| --------------------- | -------------------: |
| Dataset               | NYC Airbnb Open Data |
| Year                  |                 2019 |
| Rows                  |               48,895 |
| Columns               |                   16 |
| Location              |        New York City |
| Minimum Required Rows |               20,000 |

The dataset contains significantly more records than the coursework requirement of 20,000 rows.

### Dataset Source

The source mirror used for this coursework is:

```text
https://raw.githubusercontent.com/pjournal/boun01g-data-mine-r-s/gh-pages/Assignment/AB_NYC_2019.csv
```

The dataset was originally published through sources including **Kaggle / Inside Airbnb**.

---

#  3. Prerequisites

Before running the project, install:

* **Python 3.10+**
* **PostgreSQL 14+**
* **Apache Airflow 2.9+** — required only when running the pipeline through Airflow

The pipeline logic itself does not depend on Airflow and can be executed directly using:

```text
scripts/run_pipeline.py
```

---

## Install Python Dependencies

```bash
pip install -r requirements.txt
```

---

#  4. Database Setup

## 4.1 Create the Database

Create the PostgreSQL database:

```bash
psql -U postgres -c "CREATE DATABASE airbnb_dw;"
```

---

## 4.2 Configure Database Connection

Set the PostgreSQL environment variables.

### Linux / macOS

```bash
export PG_HOST=localhost
export PG_PORT=5432
export PG_DB=airbnb_dw
export PG_USER=postgres
export PG_PASSWORD=postgres
```

### Windows PowerShell

```powershell
$env:PG_HOST="localhost"
$env:PG_PORT="5432"
$env:PG_DB="airbnb_dw"
$env:PG_USER="postgres"
$env:PG_PASSWORD="postgres"
```

The default configuration is:

```text
Host:     localhost
Port:     5432
Database: airbnb_dw
User:     postgres
Password: postgres
```

>  Change the password to match your local PostgreSQL configuration.

The database schema is automatically created by the first pipeline stage using:

```text
sql/01_schema.sql
```

You do not need to manually execute the schema file before running the pipeline.

---

#  5. Running the Pipeline

There are two ways to execute the pipeline.

---

## Option A — Standalone Python Pipeline

This is the fastest way to verify that the complete pipeline works without setting up Airflow.

Navigate to the project directory:

```bash
cd project
```

Set the PostgreSQL password:

```bash
export PGPASSWORD=postgres
```

Run the pipeline:

```bash
python3 scripts/run_pipeline.py
```

On Windows PowerShell:

```powershell
$env:PGPASSWORD="postgres"
python scripts/run_pipeline.py
```

The standalone runner executes all pipeline stages in sequence and displays row counts throughout the process.

---

## Option B — Apache Airflow

The project can also be executed using an Apache Airflow DAG.

### 1. Configure the Airflow DAG

Point Airflow to the project's:

```text
dags/
```

directory.

For example, you can configure the `dags_folder` in `airflow.cfg`.

Alternatively, copy the following directories into your Airflow environment:

```text
dags/
scripts/
sql/
data/
```

> The relative project structure must be preserved because the DAG imports the Python scripts as regular modules.

---

### 2. Configure Environment Variables

Make sure the following PostgreSQL variables are available to the Airflow worker:

```text
PG_HOST
PG_PORT
PG_DB
PG_USER
PG_PASSWORD
```

These can be configured using:

* Airflow configuration
* Environment variables
* `.env`
* Airflow Connections
* Airflow Variables

---

### 3. Initialize Airflow

Run:

```bash
airflow db migrate
```

Then start Airflow:

```bash
airflow standalone
```

---

### 4. Open Airflow UI

Open:

```text
http://localhost:8080
```

Find the DAG:

```text
airbnb_dw_pipeline
```

Unpause the DAG and trigger a new run.

---

#  Airflow Task Pipeline

The Airflow DAG contains **8 tasks** with explicit dependencies.

```text
create_schema
      │
      ▼
extract_raw_data
      │
      ▼
clean_and_validate
      │
      ▼
transform_data
      │
      ├───────────────┐
      ▼               ▼
load_dimensions    load_fact
      │               │
      ▼               │
load_marts ◄──────────┘
      │
      ▼
run_analytical_queries
```

The dependency structure is:

```text
create_schema
    >>
extract_raw_data
    >>
clean_and_validate
    >>
transform_data

transform_data
    >>
load_dimensions

transform_data
    >>
load_fact

load_dimensions
    >>
load_marts

load_fact
    >>
run_analytical_queries

load_marts
    >>
run_analytical_queries
```

---

#  6. ELT Pipeline Stages

## 1. Extract

The raw CSV dataset is loaded into:

```text
staging.raw_listings
```

Implemented in:

```text
scripts/extract.py
```

Flow:

```text
AB_NYC_2019.csv
      ↓
PostgreSQL
      ↓
staging.raw_listings
```

---

## 2. Clean & Validate

The raw dataset is cleaned and validated before transformation.

Operations include:

* Duplicate detection
* Null-value handling
* Invalid-row detection
* Data validation
* Data quality checks

Implemented in:

```text
scripts/clean.py
```

---

## 3. Transform

The cleaned data is transformed into analytics-ready structures.

Transformations include:

* Derived columns
* Date processing
* Data type conversions
* Aggregations
* Business-oriented transformations

Implemented in:

```text
scripts/transform.py
```

---

## 4. Load

The transformed data is loaded into a PostgreSQL **star schema** consisting of:

* Dimension tables
* Fact table
* Analytical data marts

Implemented in:

```text
scripts/load.py
```

---

#  7. Data Warehouse

The project uses a **PostgreSQL star schema** to organize the transformed Airbnb data for analytical queries.

Conceptually:

```text
                  ┌──────────────────────┐
                  │  dim_neighbourhood   │
                  └──────────┬───────────┘
                             │
                             │
┌────────────────┐           ▼
│    dim_host    │──────► fact_listings ◄────── dim_room_type
└────────────────┘           │
                             │
                             ▼
                    ┌──────────────────┐
                    │ Analytical Marts │
                    └──────────────────┘
```

The warehouse separates descriptive dimensions from measurable listing facts, making the data easier to analyze using SQL.

---

#  8. Analytical Queries

The project includes **6 analytical business-insight queries** located in:

```text
sql/02_analytical_queries.sql
```

These queries are designed to demonstrate how the warehouse can be used to answer business-oriented questions.

Run the queries using:

```bash
psql -U postgres -d airbnb_dw -f sql/02_analytical_queries.sql
```

---

## Example Analytical Query

The following query calculates the number of listings and average price by neighbourhood group:

```sql
SELECT
    n.neighbourhood_group,
    COUNT(*) AS listing_count,
    ROUND(AVG(f.price), 2) AS avg_price
FROM core.fact_listings f
JOIN core.dim_neighbourhood n
    USING (neighbourhood_key)
GROUP BY 1
ORDER BY 2 DESC;
```

This can be used to compare the distribution and average pricing of Airbnb listings across New York City neighbourhood groups.

---

#  9. Verifying the Results

After the pipeline completes successfully, verify the warehouse using PostgreSQL.

Connect to the database:

```bash
psql -U postgres -d airbnb_dw
```

Check the staging table:

```sql
SELECT COUNT(*)
FROM staging.raw_listings;
```

Check the fact table:

```sql
SELECT COUNT(*)
FROM core.fact_listings;
```

Check neighbourhood-level results:

```sql
SELECT
    n.neighbourhood_group,
    COUNT(*) AS listing_count,
    ROUND(AVG(f.price), 2) AS avg_price
FROM core.fact_listings f
JOIN core.dim_neighbourhood n
    USING (neighbourhood_key)
GROUP BY 1
ORDER BY 2 DESC;
```

---

#  10. Re-running & Idempotency

The pipeline is designed to be safely re-run from the beginning.

The file:

```text
sql/01_schema.sql
```

drops and recreates the following schemas:

```text
staging
core
marts
```

Therefore, every complete pipeline execution starts from a clean database state.

```text
Previous Data
     ↓
Drop Schemas
     ↓
Recreate Schemas
     ↓
Extract
     ↓
Clean
     ↓
Transform
     ↓
Load
     ↓
Analytics
```

This makes each pipeline execution **idempotent with respect to the source CSV**.

---

## Production Improvement

For a production or incremental version of the pipeline, the full schema rebuild could be replaced with an incremental loading strategy.

For example:

```sql
INSERT INTO ...
VALUES (...)
ON CONFLICT (listing_id)
DO UPDATE SET ...;
```

The `listing_id` would be used as the business key for incremental upsert operations.

This would allow the pipeline to process only new or updated listings instead of rebuilding the entire warehouse.

---

#  11. Key Data Engineering Concepts Demonstrated

This project demonstrates practical implementation of:

* **ELT pipeline design**
* **Data extraction**
* **Data cleaning and validation**
* **Data transformation**
* **Data loading**
* **PostgreSQL**
* **Star schema**
* **Dimensional modeling**
* **Fact and dimension tables**
* **Data marts**
* **SQL analytics**
* **ETL/ELT orchestration**
* **Apache Airflow**
* **Pipeline dependencies**
* **Data quality validation**
* **Idempotent pipeline execution**
* **Python-based data engineering**

---

#  12. Project Documentation

The technical documentation for the coursework is available in:

```text
report/
└── Technical_Report.docx
```

The report contains additional details about the pipeline design, implementation, transformations, and analytical requirements.

---

#  13. Author

**Diviyan Periyasamy**

**Coursework 1 — Data Engineering Module**
**2026**
---

⭐ If you find this project useful, consider giving the repository a star!
