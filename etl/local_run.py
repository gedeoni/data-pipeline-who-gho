from __future__ import annotations
import argparse
import logging
import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.config import ETLConfig
from etl.extract import ODataClient, fetch_observations
from etl.transform import transform_observations, transform_indicators, transform_countries
from etl.validate import validate_dataframe, Observation, Indicator, Country
from etl.load import get_db_engine, create_schema, bulk_upsert, save_rejected_records
from etl.state import EtlStateRepository

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def load_dotenv(path: str | None = None) -> None:
    if path is None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        path = os.path.join(repo_root, ".env")
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())

def main(args):
    """Main function for the local ETL run."""

    # --- CONFIG ---
    # In a real local run, you might use a .env file or other config method
    # For simplicity, we'll use hardcoded values that mirror Airflow Variables
    config = ETLConfig.for_local_dev()
    if args.limit is not None:
        config = ETLConfig(who_odata=config.who_odata, dev_run_limit=args.limit)

    # --- DB & STATE ---
    # Make sure to set the connection string as an environment variable
    load_dotenv()
    analytics_db_conn_str = os.environ.get("ANALYTICS_DB_CONN_STR")
    if not analytics_db_conn_str:
        raise ValueError("ANALYTICS_DB_CONN_STR environment variable not set.")

    engine = get_db_engine(analytics_db_conn_str)
    create_schema(engine)
    EtlStateRepository.ensure_etl_state_table_exists(engine)

    from sqlalchemy.orm import sessionmaker
    Session = sessionmaker(bind=engine)
    session = Session()
    state_repo = EtlStateRepository(session)

    # --- EXTRACT ---
    client = ODataClient(config.who_odata.base_url, state_repo=state_repo)

    log.info("Extracting indicators...")
    indicators_raw = list(client.get_all_data("Indicator", "who_indicators"))

    log.info("Extracting countries...")
    countries_raw = list(
        client.get_all_data("DIMENSION/COUNTRY/DimensionValues", "who_countries")
    )

    log.info("Extracting observations...")
    # If specific indicator codes are not provided in the config, fall back to
    # using all indicators that were just extracted.
    observation_indicator_codes = config.who_odata.indicator_codes
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
        limit=config.dev_run_limit
    )

    # --- TRANSFORM & VALIDATE ---
    indicators_df = transform_indicators(indicators_raw)
    countries_df = transform_countries(countries_raw)
    observations_df = transform_observations(observations_raw)

    validated_indicators = validate_dataframe(indicators_df, Indicator)
    validated_countries = validate_dataframe(countries_df, Country)
    validated_observations = validate_dataframe(observations_df, Observation)

    # --- LOAD ---
    log.info("Loading indicators...")
    bulk_upsert(engine, "dim_indicator", [p.model_dump() for p in validated_indicators.validated_records], "indicator_code")
    save_rejected_records(engine, validated_indicators.rejected_records)

    log.info("Loading countries...")
    bulk_upsert(engine, "dim_country", [p.model_dump() for p in validated_countries.validated_records], "country_code")
    save_rejected_records(engine, validated_countries.rejected_records)

    log.info("Loading observations...")
    bulk_upsert(
        engine,
        "fact_observation",
        [p.model_dump() for p in validated_observations.validated_records],
        "observation_id",
    )
    save_rejected_records(engine, validated_observations.rejected_records)

    log.info("ETL run finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Local WHO GHO ETL runner.")
    parser.add_argument("--limit", type=int, help="Limit the number of observation records for a dev run.")
    args = parser.parse_args()
    main(args)
