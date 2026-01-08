from __future__ import annotations
import os
from dataclasses import dataclass

try:
    from airflow.models import Variable
except ModuleNotFoundError:  # pragma: no cover - optional dependency for local runs
    Variable = None

@dataclass(frozen=True)
class WHOODataConfig:
    """Configuration for the WHO OData API."""
    base_url: str
    indicator_codes: list[str]

@dataclass(frozen=True)
class ETLConfig:
    """Main configuration for the ETL pipeline."""
    who_odata: WHOODataConfig
    dev_run_limit: int | None = None

    @classmethod
    def from_airflow_variables(cls) -> ETLConfig:
        """Hydrates the config from Airflow Variables."""
        if Variable is None:
            raise ModuleNotFoundError(
                "airflow is required for from_airflow_variables; install airflow or use for_local_dev."
            )
        who_base_url = Variable.get("who_gho_base_url", default_var="https://ghoapi.azureedge.net/api")
        # Default to an empty string. An empty list signals that all indicators should be used.
        indicator_codes_str = Variable.get("who_gho_indicator_codes", default_var="")

        return cls(
            who_odata=WHOODataConfig(
                base_url=who_base_url,
                # Filter out empty strings that can result from an empty default_var
                indicator_codes=[code for code in indicator_codes_str.split(",") if code],
            )
        )

    @classmethod
    def for_local_dev(cls) -> ETLConfig:
        """Returns a config with hardcoded values for local development."""
        return cls(
            who_odata=WHOODataConfig(
                base_url="https://ghoapi.azureedge.net/api",
                indicator_codes=["WHOSIS_000001", "LIFE_EXPECTANCY_0"],
            )
        )
    
    
