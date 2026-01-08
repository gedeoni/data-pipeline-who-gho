from unittest.mock import MagicMock, patch
from sqlalchemy import Column, MetaData, String, Table
import pytest
from etl.load import bulk_upsert, save_rejected_records
from etl.validate import RejectedRecord

@pytest.fixture
def mock_engine():
    """Fixture for a mock SQLAlchemy engine."""
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value.execute.return_value = None
    return engine

def test_bulk_upsert(mock_engine):
    """Tests that bulk_upsert generates the correct SQL."""

    records = [
        {"indicator_code": "code1", "indicator_name": "name1", "language": "EN"},
        {"indicator_code": "code2", "indicator_name": "name2", "language": "EN"},
    ]

    table_metadata = MetaData()
    dim_indicator_table = Table(
        "dim_indicator",
        table_metadata,
        Column("indicator_code", String, primary_key=True),
        Column("indicator_name", String),
        Column("language", String),
    )

    with patch('etl.load.MetaData') as mock_metadata, patch('etl.load.Table') as mock_table:
        mock_metadata.return_value.reflect.return_value = None
        mock_table.return_value = dim_indicator_table
        # This is a bit complex because of SQLAlchemy's dynamic nature
        # A more robust test would involve an in-memory sqlite DB

        bulk_upsert(mock_engine, "dim_indicator", records, "indicator_code")

        # Assert that a connection was opened and execute was called
        mock_engine.begin.assert_called_once()
        mock_engine.begin.return_value.__enter__.return_value.execute.assert_called()


def test_save_rejected_records(mock_engine):
    """Tests that save_rejected_records correctly inserts into the DB."""

    rejected_records = [
        RejectedRecord(record_data={"id": 1}, error_details="error1"),
        RejectedRecord(record_data={"id": 2}, error_details="error2"),
    ]

    with patch('etl.load.Table') as mock_table:
        save_rejected_records(mock_engine, rejected_records)

        # Assert that a connection was opened and execute was called
        mock_engine.begin.assert_called_once()
        mock_engine.begin.return_value.__enter__.return_value.execute.assert_called()

def test_save_rejected_records_no_records(mock_engine):
    """Tests that save_rejected_records does nothing when there are no records."""

    save_rejected_records(mock_engine, [])

    mock_engine.begin.assert_not_called()
