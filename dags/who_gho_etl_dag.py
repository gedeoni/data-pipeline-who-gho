from __future__ import annotations

import logging
from datetime import datetime
import json
import os

import pandas as pd

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context

from etl.config import ETLConfig
from etl.extract import ODataClient, fetch_observations
from etl.transform import transform_observations, transform_indicators, transform_countries, save_validated_data
from etl.validate import validate_dataframe, Observation, Indicator, Country
from etl.load import get_db_engine, create_schema, bulk_upsert, save_rejected_records
from etl.state import EtlStateRepository, ensure_etl_state_table_exists

log = logging.getLogger(__name__)

def get_analytics_engine():
    """Gets a SQLAlchemy engine for the analytics DB."""
    hook = PostgresHook(postgres_conn_id="postgres_analytics")
    return hook.get_sqlalchemy_engine()

@dag(
    dag_id="who_gho_etl",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    params={
        "dev_run_limit": Param(None, type=["null", "integer"], description="Limit the number of records for a development run."),
        "full_reingest": Param(False, type="boolean", description="If true, ignores watermarks and re-ingests all data."),
    },
    doc_md="""
    ### WHO GHO ETL Pipeline
    This DAG extracts data from the WHO GHO OData API, transforms it, and loads it into a PostgreSQL database.
    """,
    email_on_failure=True,
    email=['your_email@example.com'], # Configure in airflow.cfg
)
def who_gho_etl_dag():

    @task
    def get_etl_config() -> dict:
        """Reads config from Airflow and returns it as a dictionary."""
        context = get_current_context()
        params = context["params"]

        config = ETLConfig.from_airflow_variables()
        config.dev_run_limit = params.get("dev_run_limit")

        return config.__dict__

    @task
    def extract_data(config: dict) -> dict[str, str]:
        """
        Extracts data from the WHO GHO API and saves it to local files.
        Returns a dictionary of file paths.
        """
        run_id = get_current_context()["run_id"]
        data_dir = f"/opt/airflow/data/{run_id}"
        os.makedirs(data_dir, exist_ok=True)

        etl_config = ETLConfig(**config)
        engine = get_analytics_engine()
        ensure_etl_state_table_exists(engine)

        from sqlalchemy.orm import sessionmaker
        Session = sessionmaker(bind=engine)
        session = Session()
        state_repo = EtlStateRepository(session)

        client = ODataClient(etl_config.who_odata.base_url, state_repo=state_repo)

        output_paths = {}

        # Extract Indicators
        indicators_raw = list(client.get_all_data("Indicator", "who_indicators"))
        indicators_path = f"{data_dir}/indicators.json"
        with open(indicators_path, "w") as f:
            json.dump(indicators_raw, f)
        output_paths["indicators"] = indicators_path

        # Extract Countries
        countries_raw = list(
            client.get_all_data("DIMENSION/COUNTRY/DimensionValues", "who_countries")
        )
        countries_path = f"{data_dir}/countries.json"
        with open(countries_path, "w") as f:
            json.dump(countries_raw, f)
        output_paths["countries"] = countries_path

        # Extract Observations
        # If specific indicator codes are not provided in the config, fall back to using all indicators that were just extracted.
        observation_indicator_codes = etl_config.who_odata.indicator_codes
        if not observation_indicator_codes:
            log.info("No indicator codes provided in config. Falling back to all extracted indicators.")
            observation_indicator_codes = [ind["IndicatorCode"] for ind in indicators_raw if ind.get("IndicatorCode")]

        country_codes = [
            country["Code"] for country in countries_raw if country.get("Code")
        ]

        observations_raw = fetch_observations(
            client,
            observation_indicator_codes,
            country_codes,
            limit=etl_config.dev_run_limit
        )

        observations_path = f"{data_dir}/observations.json"
        with open(observations_path, "w") as f:
            json.dump(observations_raw, f)
        output_paths["observations"] = observations_path

        return output_paths

    @task
    def transform_and_validate(file_paths: dict[str, str]) -> dict[str, str]:
        """Transforms and validates the extracted data."""
        run_id = get_current_context()["run_id"]
        processed_dir = f"/opt/airflow/data/{run_id}/processed"
        os.makedirs(processed_dir, exist_ok=True)

        output_paths = {}

        # Process Indicators
        with open(file_paths["indicators"]) as f:
            indicators_raw = json.load(f)
        indicators_df = transform_indicators(indicators_raw)
        validated_indicators = validate_dataframe(indicators_df, Indicator)

        # Process Countries
        with open(file_paths["countries"]) as f:
            countries_raw = json.load(f)
        countries_df = transform_countries(countries_raw)
        validated_countries = validate_dataframe(countries_df, Country)

        # Process Observations
        with open(file_paths["observations"]) as f:
            observations_raw = json.load(f)
        observations_df = transform_observations(observations_raw)
        validated_observations = validate_dataframe(observations_df, Observation)

        # Save validated data
        output_paths["validated_indicators"] = save_validated_data(validated_indicators, processed_dir, "validated_indicators.json")
        output_paths["validated_countries"] = save_validated_data(validated_countries, processed_dir, "validated_countries.json")
        output_paths["validated_observations"] = save_validated_data(validated_observations, processed_dir, "validated_observations.json")

        return output_paths


    @task
    def load_data(file_paths: dict[str, str]):
        """Loads the transformed data into the analytics database."""
        engine = get_analytics_engine()
        create_schema(engine)

        # Load Indicators
        with open(file_paths["validated_indicators"]) as f:
            indicators = json.load(f)
        bulk_upsert(engine, "dim_indicator", indicators, "indicator_code")

        # Load Countries
        with open(file_paths["validated_countries"]) as f:
            countries = json.load(f)
        bulk_upsert(engine, "dim_country", countries, "country_code")

        # Load Observations
        with open(file_paths["validated_observations"]) as f:
            observations = json.load(f)
        bulk_upsert(engine, "fact_observation", observations, "id")

    @task
    def data_quality_checks():
        """Runs data quality checks on the loaded data."""
        engine = get_analytics_engine()
        with engine.connect() as connection:
            # Check 1: Row count in fact_observation
            result = connection.execute("SELECT COUNT(*) FROM fact_observation").scalar()
            if result == 0:
                raise ValueError("Data quality check failed: fact_observation has no rows.")
            log.info(f"Data quality check passed: fact_observation has {result} rows.")

            # Check 2: No nulls in key columns
            result = connection.execute("SELECT COUNT(*) FROM fact_observation WHERE indicator_code IS NULL OR country_code IS NULL OR year IS NULL").scalar()
            if result > 0:
                raise ValueError("Data quality check failed: fact_observation has nulls in key columns.")
            log.info("Data quality check passed: fact_observation has no nulls in key columns.")

    # --- DAG execution ---
    config_dict = get_etl_config()
    extracted_files = extract_data(config_dict)
    transformed_files = transform_and_validate(extracted_files)
    load_op = load_data(transformed_files)
    dq_checks = data_quality_checks()

    load_op >> dq_checks

who_gho_etl_dag()
