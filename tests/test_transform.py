import pandas as pd
from etl.transform import transform_observations, transform_indicators, transform_countries

def test_transform_observations(raw_observations_data):
    df = transform_observations(raw_observations_data)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 5 # One duplicate is removed
    assert "NumericValue" in df.columns
    assert df["NumericValue"].dtype == "float64"
    assert "TimeDim" in df.columns
    assert df["TimeDim"].dtype == "Int64"
    assert "SpatialDimType" in df.columns
    assert "TimeDimType" in df.columns
    assert "Value" in df.columns

    # Check year parsing
    assert df[df["SpatialDim"] == "GBR"]["TimeDim"].iloc[0] == 2020

def test_transform_indicators(raw_indicators_data):
    df = transform_indicators(raw_indicators_data)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2 # One duplicate is removed
    assert list(df.columns) == ["IndicatorCode", "IndicatorName", "Language"]

def test_transform_countries(raw_countries_data):
    df = transform_countries(raw_countries_data)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3 # One duplicate is removed
    assert list(df.columns) == ["Code", "Title"]
