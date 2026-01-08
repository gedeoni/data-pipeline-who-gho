import pytest
import os
import sys

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture
def raw_observations_data():
    return [
        {"Id": "1", "IndicatorCode": "WHOSIS_000001", "SpatialDim": "USA", "SpatialDimType": "COUNTRY", "TimeDim": "2019", "TimeDimType": "YEAR", "NumericValue": "78.5", "Value": "78.5"},
        {"Id": "2", "IndicatorCode": "WHOSIS_000001", "SpatialDim": "CAN", "SpatialDimType": "COUNTRY", "TimeDim": "2019", "TimeDimType": "YEAR", "NumericValue": "82.0", "Value": "82.0"},
        {"Id": "3", "IndicatorCode": "LIFE_EXPECTANCY_0", "SpatialDim": "MEX", "SpatialDimType": "COUNTRY", "TimeDim": "2018", "TimeDimType": "YEAR", "NumericValue": "75.0", "Value": "75.0"},
        {"Id": "1", "IndicatorCode": "WHOSIS_000001", "SpatialDim": "USA", "SpatialDimType": "COUNTRY", "TimeDim": "2019", "TimeDimType": "YEAR", "NumericValue": "78.5", "Value": "78.5"}, # Duplicate
        {"Id": "4", "IndicatorCode": "NO_DATA", "SpatialDim": "NON", "SpatialDimType": "COUNTRY", "TimeDim": "2022", "TimeDimType": "YEAR", "NumericValue": None, "Value": "No data"},
        {"Id": "5", "IndicatorCode": "WHOSIS_000001", "SpatialDim": "GBR", "SpatialDimType": "COUNTRY", "TimeDim": "2020-2020", "TimeDimType": "YEAR", "NumericValue": "80.4", "Value": "80.4"},
    ]

@pytest.fixture
def raw_indicators_data():
    return [
        {"IndicatorCode": "WHOSIS_000001", "IndicatorName": "Life expectancy at birth (years)", "Language": "EN"},
        {"IndicatorCode": "LIFE_EXPECTANCY_0", "IndicatorName": "Life expectancy at age 60 (years)", "Language": "EN"},
        {"IndicatorCode": "WHOSIS_000001", "IndicatorName": "Life expectancy at birth (years)", "Language": "EN"}, # Duplicate
    ]

@pytest.fixture
def raw_countries_data():
    return [
        {"Code": "USA", "Title": "United States of America"},
        {"Code": "CAN", "Title": "Canada"},
        {"Code": "MEX", "Title": "Mexico"},
        {"Code": "USA", "Title": "United States of America"}, # Duplicate
    ]
