"""Data Orchestration Repository"""

from typing import Optional, Dict, Any
from app.repository.baseapp_repository import BaseAppRepository
from app.model.data_orchestration_model import DataOrchestrationModel


class DataOrchestrationRepository(BaseAppRepository[DataOrchestrationModel]):
    """Repository for data orchestration operations."""

    def __init__(self, db):
        super().__init__(db, DataOrchestrationModel)

    async def insert(self, data: DataOrchestrationModel) -> Optional[DataOrchestrationModel]:
        """
        Insert a new data orchestration record.
        
        Args:
            data: Dictionary containing model data
            
        Returns:
            Created model instance or None
        """
        try:
            record = self.model(**data)
            self.db.add(record)
            await self.db.flush()
            await self.db.refresh(record)
            return record
        except Exception:  # pylint: disable=broad-except
            return None
