from __future__ import annotations

from sqlalchemy import Column, DateTime, Float, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class DimIndicator(Base):
    __tablename__ = "dim_indicator"

    indicator_code = Column(String, primary_key=True)
    indicator_name = Column(String)
    language = Column(String)


class DimCountry(Base):
    __tablename__ = "dim_country"

    country_code = Column(String, primary_key=True)
    country_name = Column(String)


class FactObservation(Base):
    __tablename__ = "fact_observation"

    observation_id = Column(String, primary_key=True)
    indicator_code = Column(String, ForeignKey("dim_indicator.indicator_code"), nullable=False)
    spatial_dim = Column(String, ForeignKey("dim_country.country_code"), nullable=False)
    spatial_dim_type = Column(String, nullable=False)
    time_dim = Column(String, nullable=False)
    time_dim_type = Column(String, nullable=False)
    numeric_value = Column(Float, nullable=True)
    value = Column(String, nullable=True)


class EtlState(Base):
    __tablename__ = "etl_state"

    process_name = Column(String, primary_key=True)
    last_successful_run_at = Column(DateTime, nullable=True)
    checkpoint_state = Column(JSON, nullable=True)


class RejectedRecord(Base):
    __tablename__ = "rejected_record"

    id = Column(Integer, primary_key=True, autoincrement=True)
    record_data = Column(String)
    error_details = Column(String)
