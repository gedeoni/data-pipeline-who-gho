from __future__ import annotations

import logging
from typing import List
from sqlalchemy import create_engine, Table, MetaData, text, inspect
from sqlalchemy.dialects.postgresql import insert

from etl.validate import RejectedRecord
from etl.models import Base

log = logging.getLogger(__name__)

def get_db_engine(conn_str: str):
    """Creates a SQLAlchemy engine."""
    return create_engine(conn_str)

def create_schema(engine):
    """Creates the database schema if it doesn't exist."""
    inspector = inspect(engine)
    if inspector.has_table("fact_observation"):
        columns = {col["name"] for col in inspector.get_columns("fact_observation")}
        if "observation_id" not in columns:
            log.warning("Existing fact_observation schema is outdated; recreating table.")
            with engine.begin() as conn:
                conn.execute(text("DROP TABLE IF EXISTS fact_observation"))
    Base.metadata.create_all(engine)
    log.info("Schema created or already exists.")


def bulk_upsert(engine, table_name: str, records: List[dict], unique_key: str):
    """Performs a bulk upsert operation."""
    if not records:
        log.info(f"No records to upsert for table {table_name}.")
        return

    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = Table(table_name, metadata, autoload_with=engine)

    insert_stmt = insert(table).values(records)

    # For PostgreSQL, use ON CONFLICT DO UPDATE
    update_dict = {c.name: c for c in insert_stmt.excluded if c.name != unique_key}

    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=[unique_key],
        set_=update_dict
    )

    with engine.begin() as conn:
        conn.execute(upsert_stmt)
    log.info(f"Upserted {len(records)} records into {table_name}.")


def save_rejected_records(engine, rejected_records: List[RejectedRecord]):
    """Saves rejected records to the 'rejected_records' table."""
    if not rejected_records:
        return

    records_to_insert = [
        {"record_data": str(r.record_data), "error_details": r.error_details}
        for r in rejected_records
    ]

    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = Table('rejected_record', metadata, autoload_with=engine)

    with engine.begin() as conn:
        conn.execute(table.insert(), records_to_insert)
    log.info(f"Saved {len(rejected_records)} rejected records.")
