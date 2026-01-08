# WHO GHO ETL Pipeline

This project is a production-lean Apache Airflow pipeline that extracts data from the WHO GHO OData API, transforms it, and loads it into a PostgreSQL database for analysis.

## Architecture Overview

```
+-----------------+      +----------------------+      +--------------------+
|   WHO GHO API   |----->|   Airflow (ETL)      |----->| PostgreSQL         |
| (OData Source)  |      | (Dockerized)         |      | (Analytics Target) |
+-----------------+      +----------------------+      +--------------------+
                         | - Extract (Paged)    |      | - dim_country      |
                         | - Transform/Validate |      | - dim_indicator    |
                         | - Load (Upsert)      |      | - fact_observation |
                         | - DQ Checks          |      | - etl_state        |
                         +----------------------+      | - rejected_records |
                                                       +--------------------+
```

The pipeline is implemented as an Airflow DAG using the TaskFlow API. The core logic is organized into a Python package `etl/`.

- **Extraction**: The `ODataClient` in `etl.extract` handles fetching data from the WHO API. It supports pagination and resumable extracts through checkpointing.
- **Transformation & Validation**: Raw data is transformed into a clean format by functions in `etl.transform` and validated against Pydantic models in `etl.validate`.
- **Loading**: The `etl.load` module manages creating the database schema and loading data using efficient `UPSERT` operations.
- **State Management**: The `etl.state` module provides a mechanism for storing and retrieving ETL state (watermarks and checkpoints) in a dedicated `etl_state` table in the analytics database.

## Setup Instructions

### 1. Prerequisites

- Docker and Docker Compose
- An environment that can run `bash` scripts.

### 2. Environment Setup

Create a `.env` file in the project root with the following content:

```
AIRFLOW_UID=50000
```

### 3. Build and Run the Services

```bash
docker-compose up -d --build
```

This will start the following services:
- `postgres_airflow`: PostgreSQL for the Airflow metadata database.
- `postgres_analytics`: PostgreSQL for the analytics target database.
- `airflow-init`: Initializes the Airflow database and creates a default user.
- `airflow-webserver`: The Airflow web interface, available at `http://localhost:8080`.
- `airflow-scheduler`: The Airflow scheduler, which triggers DAG runs.

### 4. Configure Airflow

#### a. Log in to the Airflow UI

- **URL**: `http://localhost:8080`
- **Username**: `admin`
- **Password**: `admin`

#### b. Create Airflow Connections

Go to **Admin -> Connections** and add a new connection:

- **Connection ID**: `postgres_analytics`
- **Connection Type**: `Postgres`
- **Host**: `postgres_analytics`
- **Schema**: `analytics`
- **Login**: `analytics`
- **Password**: `analytics`
- **Port**: `5432`

#### c. Create Airflow Variables

Go to **Admin -> Variables** and add the following variables:

- **key**: `who_gho_base_url`
  - **val**: `https://ghoapi.azureedge.net/api`
- **key**: `who_gho_indicator_codes`
  - **val**: `WHOSIS_000001,LIFE_EXPECTANCY_0` (Example indicators; each indicator is fetched per country)

## How to Run the DAG

1. In the Airflow UI, go to the **DAGs** view.
2. Find the `who_gho_etl` DAG and un-pause it.
3. To trigger a manual run, click the "Play" button on the right side. You can optionally specify parameters for the run:
    - `dev_run_limit`: For a quick test run, set this to a small number (e.g., 1000) to limit the number of observation records fetched.
    - `full_reingest`: Set to `true` to ignore any saved watermarks and re-ingest all data from the beginning.

## Database Schema

The ETL process loads data into a star-schema-like model in the `analytics` database.

- `dim_country`: Stores country information.
- `dim_indicator`: Stores indicator metadata.
- `fact_observation`: The main fact table containing the observations.
- `etl_state`: A utility table to manage the state of the ETL process for incremental loads and resumability.
- `rejected_records`: Stores records that failed validation, along with the error details.

### Example Analytics Queries

```sql
-- Get the average life expectancy for a specific country over time
SELECT
    dc.country_name,
    fo.year,
    fo.value AS life_expectancy
FROM fact_observation fo
JOIN dim_country dc ON fo.country_code = dc.country_code
JOIN dim_indicator di ON fo.indicator_code = di.indicator_code
WHERE
    dc.country_name = 'Japan' AND
    di.indicator_name LIKE 'Life expectancy at birth%';

-- Compare the latest life expectancy across different countries
SELECT
    dc.country_name,
    fo.value
FROM fact_observation fo
JOIN dim_country dc ON fo.country_code = dc.country_code
WHERE
    fo.indicator_code = 'WHOSIS_000001' AND
    fo.year = (SELECT MAX(year) FROM fact_observation WHERE indicator_code = 'WHOSIS_000001');

```

## Testing

To run the unit tests, execute the following command from the project root:

```bash
python -m pytest
```

## Local Development and Debugging

The `etl/local_run.py` script allows you to run the ETL process locally without needing Airflow.

1. Make sure the Docker services are running (`docker-compose up -d`).
2. Set the `ANALYTICS_DB_CONN_STR` environment variable:

```bash
export ANALYTICS_DB_CONN_STR="postgresql://analytics:analytics@localhost:5433/analytics"
```

3. Run the script:

```bash
python etl/local_run.py --limit 1000
```
This will run the ETL for a limited number of records, printing detailed logs to the console.
