from __future__ import annotations
import pandas as pd
import json
import logging
from typing import List
from etl.validate import Observation, Indicator, Country

log = logging.getLogger(__name__)

def transform_observations(records: List[dict]) -> pd.DataFrame:
    """Transforms raw observation records into a clean DataFrame."""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)

    # Keep only relevant columns for the Observation model
    # df = df.rename(columns={
    #     "Id": "Id",
    #     "IndicatorCode": "IndicatorCode",
    #     "SpatialDim": "SpatialDim",
    #     "SpatialDimType": "SpatialDimType",
    #     "TimeDim": "TimeDim",
    #     "TimeDimType": "TimeDimType",
    #     "NumericValue": "NumericValue",
    #     "Value": "Value",
    # })

    # Ensure required columns exist
    for col in ["Id", "IndicatorCode", "SpatialDim", "SpatialDimType", "TimeDim", "TimeDimType", "Value"]:
        if col not in df.columns:
            df[col] = None

    # Safe conversion to numeric, coercing errors to NaN
    df["NumericValue"] = pd.to_numeric(df["NumericValue"], errors='coerce')

    # Normalize year
    df["TimeDim"] = df["TimeDim"].astype(str).str.split('-').str[0]
    df["TimeDim"] = pd.to_numeric(df["TimeDim"], errors='coerce').astype('Int64')

    # Drop rows where key identifiers are missing
    df = df.dropna(subset=["IndicatorCode", "SpatialDim", "TimeDim"])

    # Deduplicate
    if "Id" in df.columns and df["Id"].notna().any():
        df = df.drop_duplicates(subset=["Id"])
    else:
        df = df.drop_duplicates(subset=["IndicatorCode", "SpatialDim", "TimeDim"])

    log.info(f"Transformed {len(df)} observation records.")
    return df

def transform_indicators(records: List[dict]) -> pd.DataFrame:
    """Transforms raw indicator records into a clean DataFrame."""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    # df = df.rename(columns={"IndicatorCode": "IndicatorCode", "IndicatorName": "IndicatorName", "Language": "Language"})

    # Ensure required columns exist and filter out extra columns
    for col in ["IndicatorCode", "IndicatorName", "Language"]:
        if col not in df.columns:
            df[col] = None
    df = df[["IndicatorCode", "IndicatorName", "Language"]]

    df = df.drop_duplicates(subset=["IndicatorCode"])
    log.info(f"Transformed {len(df)} indicator records.")
    return df

def transform_countries(records: List[dict]) -> pd.DataFrame:
    """Transforms raw country records into a clean DataFrame."""
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    # df = df.rename(columns={"Code": "Code", "Title": "Title"})
    df = df.drop_duplicates(subset=["Code"])
    log.info(f"Transformed {len(df)} country records.")
    return df

def save_validated_data(validation_result, processed_dir: str, filename: str) -> str:
    """Saves validated records to a JSON file."""
    path = f"{processed_dir}/{filename}"
    with open(path, "w") as f:
        json.dump([p.model_dump() for p in validation_result.validated_records], f)
    return path
