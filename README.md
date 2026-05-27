# NYC Yellow Taxi Data Pipeline

An end-to-end data engineering pipeline that ingests, loads, and transforms **NYC Yellow Taxi trip data from 2020–2025** using Apache Airflow, DuckDB, and dbt — all containerized with Docker.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Data Models](#data-models)
- [Querying the Database](#querying-the-database)
- [Running the Full Pipeline](#running-the-full-pipeline)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
  - [Trigger the DAG](#trigger-the-dag)
- [dbt Docs](#dbt-docs)
- [Project Structure](#project-structure)

---

## Overview

This pipeline automates the full lifecycle of NYC Yellow Taxi data:

1. **Extract** — Downloads monthly Parquet files from the [NYC TLC website](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) for every month between January 2020 and December 2025
2. **Load** — Ingests all Parquet files into a local DuckDB database as a single `yellow_taxi` table
3. **Transform** — Runs dbt models to clean, rename, aggregate, and analyze the data

> **Note:** The raw Parquet files and DuckDB database are **not** included in the repository due to their size. You must [run the pipeline](#running-the-full-pipeline) first to download the data and populate the database before querying it.

---

## Architecture

```
NYC TLC Website (Parquet files)
        │
        ▼
┌──────────────────┐
│  Apache Airflow  │  Orchestrates the pipeline on a monthly schedule
│  (Docker)        │
└──────┬───────────┘
       │
       ├── Task 1: Extract ──► Download .parquet files → data/raw/
       │
       ├── Task 2: Load ────► Load all .parquet files → DuckDB (yellow_taxi table)
       │
       └── Task 3: dbt run ─► Transform data into staging views + mart tables
                                        │
                              ┌─────────┴──────────┐
                              │     DuckDB          │
                              │  data/raw/          │
                              │  database.duckdb    │
                              └────────────────────┘
```

The Airflow cluster runs on **CeleryExecutor** with Redis as the message broker and PostgreSQL as the metadata database.

---

## Tech Stack

| Tool | Version | Role |
|---|---|---|
| [Apache Airflow](https://airflow.apache.org/) | 2.11.2 | Pipeline orchestration |
| [DuckDB](https://duckdb.org/) | 1.2.2 | Embedded analytical database |
| [dbt-duckdb](https://github.com/duckdb/dbt-duckdb) | 1.9.2 | Data transformation |
| [Docker + Docker Compose](https://www.docker.com/) | — | Containerization |
| [Redis](https://redis.io/) | 7.2 | Celery message broker |
| [PostgreSQL](https://www.postgresql.org/) | 13 | Airflow metadata database |

---

## Data Models

The dbt project produces three layers of models:

### Staging (materialized as **Views**)

| Model | Description |
|---|---|
| `stg_yellow_taxi` | Cleans and renames raw columns; filters out trips with 0 passengers or 0 distance |

### Marts (materialized as **Tables**)

| Model | Description |
|---|---|
| `fct_yellow_taxi` | Daily aggregates — total trips, average distance, average fare, average tip, total revenue |
| `yellow_taxi_yearly_trips_amounts` | Year-over-year rollup of total trips and total revenue |
| `yellow_taxi_2020_monthly_percentage_drops` | Month-over-month % drop in trips and revenue throughout 2020 (COVID impact analysis) |

---

## Querying the Database

Once you have [run the pipeline](#running-the-full-pipeline), the DuckDB database will be available at `data/raw/database.duckdb` and ready to query.

### Option 1 — Python

```python
import duckdb

con = duckdb.connect("data/raw/database.duckdb")

# View all existing tables and views in the database
print(con.execute("SHOW ALL TABLES").df())

# Daily revenue summary
con.sql("SELECT * FROM fct_yellow_taxi LIMIT 10").show()

# Year-over-year totals
con.sql("SELECT * FROM yellow_taxi_yearly_trips_amounts").show()

# 2020 COVID impact
con.sql("SELECT * FROM yellow_taxi_2020_monthly_percentage_drops").show()

# Raw data
con.sql("SELECT * FROM yellow_taxi LIMIT 5").show()
```

You can also run the included `test.py` script from the project root as a quick way to print all existing relations:

```bash
python test.py
```

### Option 2 — DuckDB CLI

```bash
duckdb data/raw/database.duckdb
```

```sql
-- Inside the DuckDB shell
SHOW TABLES;
SELECT * FROM yellow_taxi_yearly_trips_amounts;
```

### Option 3 — Any DuckDB-compatible tool

Tools like [Harlequin](https://harlequin.sh/), [DBeaver](https://dbeaver.io/), or [Tableau](https://www.tableau.com/) can connect directly to a `.duckdb` file.

---

## Running the Full Pipeline

Follow the steps below to stand up the stack, trigger the pipeline, and populate the database.

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (with at least **4 GB RAM** and **2 CPUs** allocated)
- [Docker Compose](https://docs.docker.com/compose/) (included with Docker Desktop)
- ~**30 GB** of free disk space for the raw Parquet files (2020–2025)

### Setup

**1. Clone the repository**

```bash
git clone https://github.com/your-username/NYC-Taxi-Data-Pipeline.git
cd NYC-Taxi-Data-Pipeline
```

**2. Create a `.env` file** (Linux/macOS only — sets the Airflow user ID)

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
```

On Windows, skip this step or create a `.env` file with:

```
AIRFLOW_UID=50000
```

**3. Build the custom Airflow image and start all services**

```bash
docker compose up --build -d
```

This starts:
- Airflow webserver → [http://localhost:8080](http://localhost:8080)
- Airflow scheduler, worker, and triggerer
- PostgreSQL (Airflow metadata)
- Redis (Celery broker)
- dbt Docs server → [http://localhost:8082](http://localhost:8082)

First startup can take **2–5 minutes** while services initialize.

**4. Verify all containers are healthy**

```bash
docker compose ps
```

All services should show `healthy` or `running`.

### Trigger the DAG

**1. Open the Airflow UI**

Navigate to [http://localhost:8080](http://localhost:8080) in your browser. Log in with the default credentials:
- **Username:** `airflow`
- **Password:** `airflow`

**2. Find the DAG**

On the **DAGs** page you'll see a list of all available DAGs. Look for `yellow_taxi_pipeline`. It will be paused by default (indicated by a grey toggle on the left side of the row).

**3. Unpause the DAG**

Click the **toggle** to the left of `yellow_taxi_pipeline` to turn it on. It will turn blue when active. Airflow won't schedule or run any DAG while it's paused.

**4. Trigger a manual run**

On the right side of the DAG row, click the **▶ (Trigger DAG)** button. In the dialog that appears, click **Trigger** to confirm. This queues an immediate run without waiting for the next scheduled interval.

**5. Monitor the run**

Click on the DAG name (`yellow_taxi_pipeline`) to open its detail page, then select the **Grid** tab. You'll see the run appear as a column with three task boxes — one for each step:

```
extract_taxi_data  ──►  load_data_to_db  ──►  dbt_run
```

Task box colors indicate status:

| Color | Status |
|---|---|
| 🟡 Yellow | Queued |
| 🟢 Light green | Running |
| ✅ Dark green | Success |
| 🔴 Red | Failed |

**6. View task logs**

If a task fails or you want to see progress, click the task box in the Grid view and select **Logs** from the popup panel. This shows the full stdout output for that task — useful for debugging download errors or dbt failures.

> **Note:** The `extract_taxi_data` task downloads 72 monthly Parquet files (2020–2025) and will take a while depending on your internet connection. The files are skipped if they already exist locally, so re-runs are safe.

**To stop all services:**

```bash
docker compose down
```

To also remove the PostgreSQL volume (clears Airflow metadata):

```bash
docker compose down -v
```

---

## dbt Docs

When running the full stack, a dbt documentation site is served at [http://localhost:8082](http://localhost:8082). It provides an interactive lineage graph and full model documentation.

To generate and serve dbt docs locally (outside Docker), run inside the `dbt/` directory:

```bash
cd dbt
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir . --port 8082
```

---

## Project Structure

```
NYC-Taxi-Data-Pipeline/
├── dags/
│   ├── data_extract.py        # Download + load functions
│   └── dbt_dag.py             # Airflow DAG definition
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_yellow_taxi.sql
│   │   │   └── schema.yml
│   │   └── marts/
│   │       ├── fct_yellow_taxi.sql
│   │       ├── yellow_taxi_yearly_trips_amounts.sql
│   │       ├── yellow_taxi_2020_monthly_percentage_drops.sql
│   │       └── schema.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── data/
│   └── raw/                   # Populated after running the pipeline
│       ├── database.duckdb
│       └── yellow_tripdata_*.parquet
├── Dockerfile                 # Custom Airflow image with dbt + dependencies
├── docker-compose.yaml        # Full stack definition
└── requirements.txt           # Python dependencies
```

---

## Data Source

Raw data comes from the [NYC Taxi & Limousine Commission (TLC) Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Files follow the naming pattern:
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YEAR}-{MONTH}.parquet
```
