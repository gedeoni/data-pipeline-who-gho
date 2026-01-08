from __future__ import annotations
from typing import Optional, Generic, TypeVar, List
from pydantic import BaseModel, Field, field_validator
import pandas as pd

class Indicator(BaseModel):
    """Represents a WHO GHO indicator."""
    indicator_code: str = Field(..., alias="IndicatorCode")
    indicator_name: str = Field(..., alias="IndicatorName")
    language: str = Field(..., alias="Language")

    class Config:
        populate_by_name = True
        coerce_numbers_to_str = True

class Country(BaseModel):
    """Represents a country."""
    country_code: str = Field(..., alias="Code")
    country_name: str = Field(..., alias="Title")

    class Config:
        populate_by_name = True
        coerce_numbers_to_str = True

class Observation(BaseModel):
    """Represents a single observation fact."""
    observation_id: str = Field(..., alias="Id")
    indicator_code: str = Field(..., alias="IndicatorCode")
    spatial_dim: str = Field(..., alias="SpatialDim")
    spatial_dim_type: str = Field(..., alias="SpatialDimType")
    time_dim: str = Field(..., alias="TimeDim")
    time_dim_type: str = Field(..., alias="TimeDimType")
    numeric_value: Optional[float] = Field(None, alias="NumericValue")
    value: Optional[str] = Field(None, alias="Value")

    @field_validator("time_dim", mode='before')
    @classmethod
    def clean_year(cls, v):
        # some years can be like "2019-2019"
        if isinstance(v, str) and '-' in v:
            return v.split('-')[0]
        return v

    class Config:
        populate_by_name = True
        coerce_numbers_to_str = True


class RejectedRecord(BaseModel):
    """Represents a record that failed validation."""
    record_data: dict
    error_details: str

T = TypeVar("T", bound=BaseModel)

class ValidationResult(BaseModel, Generic[T]):
    """Container for validated and rejected records."""
    validated_records: List[T]
    rejected_records: List[RejectedRecord]

def validate_dataframe(df: pd.DataFrame, model: T) -> ValidationResult[T]:
    """Validates a pandas DataFrame against a Pydantic model."""
    validated_records = []
    rejected_records = []

    for record in df.to_dict(orient="records"):
        try:
            validated_records.append(model.model_validate(record))
        except Exception as e:
            rejected_records.append(
                RejectedRecord(record_data=record, error_details=str(e))
            )

    return ValidationResult(
        validated_records=validated_records,
        rejected_records=rejected_records,
    )
