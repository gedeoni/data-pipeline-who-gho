from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy import inspect
from etl.models import Base, EtlState

log = logging.getLogger(__name__)

class EtlStateRepository:
    def __init__(self, session):
        self.session = session

    def get_state(self, process_name: str) -> Optional[EtlState]:
        """Retrieves the state for a given process."""
        return self.session.query(EtlState).filter_by(process_name=process_name).first()

    def set_checkpoint_state(self, process_name: str, state: dict[str, Any]):
        """Updates the checkpoint state for a process."""
        state_obj = self.get_state(process_name)
        if not state_obj:
            state_obj = EtlState(process_name=process_name)
            self.session.add(state_obj)
        state_obj.checkpoint_state = state
        try:
            self.session.commit()
        except IntegrityError:
            # Handle concurrent inserts for the same process_name.
            self.session.rollback()
            state_obj = self.get_state(process_name)
            if not state_obj:
                raise
            state_obj.checkpoint_state = state
            self.session.commit()

    def set_last_successful_run_at(self, process_name: str, run_at: datetime):
        """Updates the last successful run timestamp."""
        state_obj = self.get_state(process_name)
        if not state_obj:
            state_obj = EtlState(process_name=process_name)
            self.session.add(state_obj)
        state_obj.last_successful_run_at = run_at
        try:
            self.session.commit()
        except IntegrityError:
            # Handle concurrent inserts for the same process_name.
            self.session.rollback()
            state_obj = self.get_state(process_name)
            if not state_obj:
                raise
            state_obj.last_successful_run_at = run_at
            self.session.commit()

    @staticmethod
    def ensure_etl_state_table_exists(engine):
        """Ensures the etl_state table exists in the database."""
        inspector = inspect(engine)
        if not inspector.has_table(EtlState.__tablename__):
            log.info(f"Table '{EtlState.__tablename__}' not found. Creating it.")
            Base.metadata.create_all(engine)
            log.info("Table created.")
        else:
            log.info(f"Table '{EtlState.__tablename__}' already exists.")
